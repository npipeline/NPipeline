using System.Collections.Frozen;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Reflection;
using NPipeline.Configuration;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Graph.Validation;
using NPipeline.Lineage;

namespace NPipeline.Pipeline;

/// <summary>
///     Pipeline build and validation methods for PipelineBuilder.
/// </summary>
public sealed partial class PipelineBuilder
{
    private const string OptimizationProfileMetadataKey = "NPipelineOptimizationProfile";

    /// <summary>
    ///     Builds the pipeline with the configured nodes, edges, and settings.
    /// </summary>
    /// <returns>A configured Pipeline instance ready for execution.</returns>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when no nodes have been added to the pipeline, or when Build has already been called on this builder
    ///     instance.
    /// </exception>
    /// <exception cref="PipelineValidationException">Thrown when graph validation fails and validation mode is set to Error.</exception>
    public Pipeline Build()
    {
        if (_built)
        {
            throw new InvalidOperationException(
                "This PipelineBuilder instance has already been built. Create a new PipelineBuilder instance if you need to build another pipeline.");
        }

        if (NodeState.Nodes.Count == 0)
            throw new InvalidOperationException(ErrorMessages.PipelineRequiresAtLeastOneNode());

        if (_config.ItemLevelLineageEnabled && !Lineage.SupportsItemLevelLineage)
            throw new InvalidOperationException(
                "Item-level lineage requires NPipeline.Extensions.Lineage. " +
                "Install the NPipeline.Extensions.Lineage package and call services.AddNPipelineLineage() " +
                "in your DI configuration.");

        if (ConfigurationState.GlobalExecutionObserver is not null)
            NodeState.ExecutionAnnotations[ExecutionAnnotationKeys.GlobalExecutionObserver] = ConfigurationState.GlobalExecutionObserver;

        // Build configuration objects from builder state
        var (errorHandlingConfig, lineageConfig, executionConfig) = BuildConfigurations();

        // Create the immutable nodes array
        var nodesList = NodeState.Nodes.Values.ToImmutableArray();

        // Create a cached frozen dictionary for O(1) node lookups during execution
        var nodeDefinitionMap = nodesList.ToFrozenDictionary(n => n.Id);

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(nodesList)
            .WithEdges(ConnectionState.Edges.ToImmutableArray())
            .WithPreconfiguredNodeInstances(NodeState.PreconfiguredNodeInstances.ToFrozenDictionary())
            .WithNodeDefinitionMap(nodeDefinitionMap)
            .WithErrorHandlingConfiguration(errorHandlingConfig)
            .WithLineageConfiguration(lineageConfig)
            .WithExecutionOptionsConfiguration(executionConfig)
            .Build();

        // Compute and attach child graphs for composite nodes
        graph = BuildChildGraphs(graph);

        if (_config.GraphValidationMode != GraphValidationMode.Off)
        {
            var allRules = _config.ExtendedValidation
                ? _customValidationRules.Concat(PipelineGraphValidator.ExtendedRules)
                : _customValidationRules.AsEnumerable();

            var validationResult = PipelineGraphValidator.Validate(graph, allRules);

            if (_config.GraphValidationMode == GraphValidationMode.Error && !validationResult.IsValid)
                throw new PipelineValidationException(validationResult);

            if (_config.GraphValidationMode == GraphValidationMode.Warn && !validationResult.IsValid)
            {
                foreach (var issue in validationResult.Issues)
                {
                    Trace.TraceWarning($"[NPipeline.Validation:{issue.Category}] {issue.Message}");
                }
            }
        }

        _built = true;
        return new Pipeline(graph) { BuilderDisposables = BuilderDisposables }; // Pipeline will adopt disposables
    }

    /// <summary>
    ///     Attempts to build the pipeline with validation, returning success status and validation result.
    /// </summary>
    /// <param name="pipeline">When this method returns, contains the built Pipeline if successful; otherwise, null.</param>
    /// <param name="validationResult">When this method returns, contains the validation result.</param>
    /// <returns>true if the pipeline was built successfully; false if validation failed or the builder has already been built.</returns>
    public bool TryBuild(out Pipeline? pipeline, out PipelineValidationResult validationResult)
    {
        pipeline = null;

        if (_built)
        {
            validationResult = new PipelineValidationResult(
                ImmutableList.Create(new ValidationIssue(ValidationSeverity.Error,
                    "This PipelineBuilder instance has already been built. Create a new PipelineBuilder instance if you need to build another pipeline.",
                    "State")));

            return false;
        }

        if (NodeState.Nodes.Count == 0)
        {
            validationResult = new PipelineValidationResult(
                ImmutableList.Create(new ValidationIssue(ValidationSeverity.Error, "A pipeline must have at least one node.", "Structure")));

            return false;
        }

        if (_config.ItemLevelLineageEnabled && !Lineage.SupportsItemLevelLineage)
        {
            validationResult = new PipelineValidationResult(
                ImmutableList.Create(new ValidationIssue(ValidationSeverity.Error,
                    "Item-level lineage requires NPipeline.Extensions.Lineage. " +
                    "Install the NPipeline.Extensions.Lineage package and call services.AddNPipelineLineage() " +
                    "in your DI configuration.",
                    "Lineage")));

            return false;
        }

        if (ConfigurationState.GlobalExecutionObserver is not null)
            NodeState.ExecutionAnnotations[ExecutionAnnotationKeys.GlobalExecutionObserver] = ConfigurationState.GlobalExecutionObserver;

        // Build configuration objects from builder state
        var (errorHandlingConfig, lineageConfig, executionConfig) = BuildConfigurations();

        // Create the immutable nodes array
        var nodesList = NodeState.Nodes.Values.ToImmutableArray();

        // Create a cached frozen dictionary for O(1) node lookups during execution
        var nodeDefinitionMap = nodesList.ToFrozenDictionary(n => n.Id);

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(nodesList)
            .WithEdges(ConnectionState.Edges.ToImmutableArray())
            .WithPreconfiguredNodeInstances(NodeState.PreconfiguredNodeInstances.ToFrozenDictionary())
            .WithNodeDefinitionMap(nodeDefinitionMap)
            .WithErrorHandlingConfiguration(errorHandlingConfig)
            .WithLineageConfiguration(lineageConfig)
            .WithExecutionOptionsConfiguration(executionConfig)
            .Build();

        // Compute and attach child graphs for composite nodes
        graph = BuildChildGraphs(graph);

        var allRules = _config.ExtendedValidation
            ? _customValidationRules.Concat(PipelineGraphValidator.ExtendedRules)
            : _customValidationRules.AsEnumerable();

        validationResult = _config.GraphValidationMode == GraphValidationMode.Off
            ? PipelineValidationResult.Success
            : PipelineGraphValidator.Validate(graph, allRules);

        if (_config.GraphValidationMode == GraphValidationMode.Error && !validationResult.IsValid)
            return false;

        _built = true;
        pipeline = new Pipeline(graph) { BuilderDisposables = BuilderDisposables };
        return true;
    }

    /// <summary>
    ///     Helper method to extract and consolidate configuration building logic.
    ///     This centralizes the creation of all configuration objects used by the pipeline graph.
    /// </summary>
    /// <returns>A tuple containing the error handling, lineage, and execution configurations.</returns>
    private (
        ErrorHandlingConfiguration ErrorHandlingConfig,
        LineageConfiguration LineageConfig,
        ExecutionOptionsConfiguration ExecutionConfig)
        BuildConfigurations()
    {
        WarnIfCompileTimeOptimizationProfileDiffers();

        var errorHandlingConfig = BuildErrorHandlingConfiguration();
        var lineageConfig = BuildLineageConfiguration();
        var executionConfig = BuildExecutionOptionsConfiguration();

        return (errorHandlingConfig, lineageConfig, executionConfig);
    }

    /// <summary>
    ///     Builds an ErrorHandlingConfiguration from the current builder state.
    /// </summary>
    private ErrorHandlingConfiguration BuildErrorHandlingConfiguration()
    {
        var retryOptions = _config.RetryOptions;
        var profileBehavior = OptimizationProfileBehaviorRegistry.For(_config.OptimizationProfile);

        if (!_config.RetryExplicitlyConfigured && profileBehavior.AutomaticRetryDefaults is not null)
            retryOptions = profileBehavior.AutomaticRetryDefaults;

        var overrideDict = NodeState.RetryOverrides.Count > 0
            ? NodeState.RetryOverrides.ToImmutableDictionary()
            : null;

        return new ErrorHandlingConfiguration
        {
            ResiliencePolicy = ConfigurationState.ResiliencePolicy,
            ResiliencePolicyType = ConfigurationState.ResiliencePolicyType,
            DeadLetterSink = ConfigurationState.DeadLetterSink,
            DeadLetterSinkType = ConfigurationState.DeadLetterSinkType,
            RetryOptions = retryOptions,
            NodeRetryOverrides = overrideDict,
            CircuitBreakerOptions = _config.CircuitBreakerOptions,
            CircuitBreakerMemoryOptions = _config.CircuitBreakerMemoryOptions,
        };
    }

    /// <summary>
    ///     Builds a LineageConfiguration from the current builder state.
    /// </summary>
    private LineageConfiguration BuildLineageConfiguration()
    {
        return new LineageConfiguration
        {
            ItemLevelLineageEnabled = _config.ItemLevelLineageEnabled,
            LineageSink = ConfigurationState.LineageSink,
            LineageSinkType = ConfigurationState.LineageSinkType,
            PipelineLineageSink = ConfigurationState.PipelineLineageSink,
            PipelineLineageSinkType = ConfigurationState.PipelineLineageSinkType,
            LineageOptions = _config.LineageOptions,
        };
    }

    /// <summary>
    ///     Builds an ExecutionOptionsConfiguration from the current builder state.
    /// </summary>
    private ExecutionOptionsConfiguration BuildExecutionOptionsConfiguration()
    {
        return new ExecutionOptionsConfiguration
        {
            NodeExecutionAnnotations = NodeState.ExecutionAnnotations.Count > 0
                ? NodeState.ExecutionAnnotations.ToImmutableDictionary()
                : null,
            Visualizer = ConfigurationState.Visualizer,
        };
    }

    private void WarnIfCompileTimeOptimizationProfileDiffers()
    {
        if (!TryResolveCompileTimeOptimizationProfile(out var compileTimeProfile))
            return;

        if (compileTimeProfile == _config.OptimizationProfile)
            return;

        Trace.TraceWarning(
            $"[NPipeline] Optimization profile mismatch detected: runtime profile '{_config.OptimizationProfile}' " +
            $"and compile-time analyzer profile '{compileTimeProfile}'. " +
            "Align PipelineBuilder.WithOptimizationProfile(...) and <NPipelineOptimizationProfile> to avoid analyzer/runtime drift.");
    }

    private bool TryResolveCompileTimeOptimizationProfile(out PipelineOptimizationProfile compileTimeProfile)
    {
        foreach (var assembly in GetOptimizationProfileMetadataCandidates())
        {
            if (TryReadOptimizationProfileMetadata(assembly, out compileTimeProfile))
                return true;
        }

        compileTimeProfile = default;
        return false;
    }

    private IEnumerable<Assembly> GetOptimizationProfileMetadataCandidates()
    {
        var seen = new HashSet<Assembly>();

        var entryAssembly = Assembly.GetEntryAssembly();

        if (entryAssembly is not null && seen.Add(entryAssembly))
            yield return entryAssembly;

        foreach (var nodeAssembly in NodeState.Nodes.Values.Select(static node => node.NodeType.Assembly))
        {
            if (seen.Add(nodeAssembly))
                yield return nodeAssembly;
        }
    }

    private static bool TryReadOptimizationProfileMetadata(Assembly assembly, out PipelineOptimizationProfile compileTimeProfile)
    {
        foreach (var metadata in assembly.GetCustomAttributes<AssemblyMetadataAttribute>())
        {
            if (!string.Equals(metadata.Key, OptimizationProfileMetadataKey, StringComparison.Ordinal))
                continue;

            var value = metadata.Value?.Trim();

            if (string.IsNullOrWhiteSpace(value))
                continue;

            if (Enum.TryParse(value, true, out compileTimeProfile))
                return true;
        }

        compileTimeProfile = default;
        return false;
    }

    /// <summary>
    ///     Scans for composite nodes with <see cref="NodeDefinition.ChildDefinitionType" /> set,
    ///     builds their child pipeline graphs, and attaches them to the parent graph.
    /// </summary>
    private static PipelineGraph BuildChildGraphs(PipelineGraph graph)
    {
        Dictionary<string, PipelineGraph>? childGraphs = null;

        foreach (var node in graph.Nodes)
        {
            if (node.Kind != NodeKind.Composite || node.ChildDefinitionType is null)
                continue;

            try
            {
                var childDef = (IPipelineDefinition)Activator.CreateInstance(node.ChildDefinitionType)!;
                var childBuilder = new PipelineBuilder();

                // Child graph extraction is a build-time operation and uses an isolated default context.
                // Child Define() implementations should remain side-effect free and fast.
                childDef.Define(childBuilder, new PipelineContext());

                if (childBuilder.TryBuild(out var childPipeline, out _) && childPipeline is not null)
                {
                    childGraphs ??= new Dictionary<string, PipelineGraph>();
                    childGraphs[node.Id] = childPipeline.Graph;
                }
            }
            catch (Exception ex) when (ex is not OutOfMemoryException and not OperationCanceledException)
            {
                // Best-effort: if child graph building fails (e.g., missing DI dependencies in Define()),
                // skip it. Consumers can still extract the graph manually.
                Trace.TraceWarning($"[NPipeline] Failed to build child graph for composite node '{node.Id}': {ex}");
            }
        }

        if (childGraphs is not null)
            return graph with { ChildGraphs = childGraphs.ToFrozenDictionary() };

        return graph;
    }
}
