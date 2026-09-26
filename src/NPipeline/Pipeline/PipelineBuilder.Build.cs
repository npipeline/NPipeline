using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Reflection;
using NPipeline.Configuration;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Graph.Validation;
using NPipeline.Nodes;
using NPipeline.Reliability;

namespace NPipeline.Pipeline;

/// <summary>
///     Pipeline build and validation methods for PipelineBuilder.
/// </summary>
public sealed partial class PipelineBuilder
{
    private const string OptimizationProfileMetadataKey = "NPipelineOptimizationProfile";

    // Resolved compile-time profile per assembly, and the (assembly, runtime profile) pairs already warned about.
    private static readonly ConcurrentDictionary<Assembly, PipelineOptimizationProfile?> CompileTimeProfileByAssembly = new();

    private static readonly ConcurrentDictionary<(Assembly Assembly, PipelineOptimizationProfile Profile), byte> ProfileMismatchWarned = new();

    /// <summary>
    ///     Number of times assembly metadata has been read while resolving the compile-time profile. Test-only; a
    ///     second resolution of the same assembly must not read its attributes again.
    /// </summary>
    internal static int AssemblyMetadataReadCount;

    /// <summary>
    ///     Clears the process-wide profile caches. Test-only: the cache is intentionally permanent in production so a
    ///     mismatch is warned about once per process.
    /// </summary>
    internal static void ResetOptimizationProfileCaches()
    {
        CompileTimeProfileByAssembly.Clear();
        ProfileMismatchWarned.Clear();
        _ = Interlocked.Exchange(ref AssemblyMetadataReadCount, 0);
    }

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
        {
            throw new InvalidOperationException(
                "Item-level lineage requires NPipeline.Extensions.Lineage. " +
                "Install the NPipeline.Extensions.Lineage package and call services.AddNPipelineLineage() " +
                "in your DI configuration.");
        }

        var graph = CreateGraph(includeChildGraphs: true);

        if (_config.GraphValidationMode != GraphValidationMode.Off)
        {
            var validationResult = PipelineGraphValidator.Validate(graph, GetValidationRules());

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

        var graph = CreateGraph(includeChildGraphs: true);

        validationResult = _config.GraphValidationMode == GraphValidationMode.Off
            ? PipelineValidationResult.Success
            : PipelineGraphValidator.Validate(graph, GetValidationRules());

        if (_config.GraphValidationMode == GraphValidationMode.Error && !validationResult.IsValid)
            return false;

        _built = true;
        pipeline = new Pipeline(graph) { BuilderDisposables = BuilderDisposables };
        return true;
    }

    /// <summary>
    ///     Assembles the pipeline graph from the current builder state, including the same error-handling, lineage and
    ///     execution configuration that <see cref="Build" /> uses.
    /// </summary>
    /// <param name="includeChildGraphs">Whether to build and attach child graphs for composite nodes.</param>
    /// <returns>The assembled graph.</returns>
    internal PipelineGraph CreateGraph(bool includeChildGraphs)
    {
        if (ConfigurationState.GlobalExecutionObserver is not null)
            NodeState.ExecutionAnnotations[ExecutionAnnotationKeys.GlobalExecutionObserver] = ConfigurationState.GlobalExecutionObserver;

        // Build configuration objects from builder state
        var (errorHandlingConfig, lineageConfig, executionConfig) = BuildConfigurations();

        // Create the immutable nodes array, with node restart applied to the transforms that configure it
        var nodesList = WithNodeRestart(NodeState.Nodes.Values, errorHandlingConfig);

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(nodesList)
            .WithEdges(ConnectionState.Edges.ToImmutableArray())
            .WithPreconfiguredNodeInstances(NodeState.PreconfiguredNodeInstances.ToFrozenDictionary())
            .WithErrorHandlingConfiguration(errorHandlingConfig)
            .WithLineageConfiguration(lineageConfig)
            .WithExecutionOptionsConfiguration(executionConfig)
            .Build();

        // Compute and attach child graphs for composite nodes
        return includeChildGraphs ? BuildChildGraphs(graph) : graph;
    }

    /// <summary>
    ///     The validation rules to apply when building: the pipeline's custom rules, plus the extended rules unless
    ///     extended validation is disabled.
    /// </summary>
    /// <returns>The ordered set of rules.</returns>
    internal IEnumerable<IGraphRule> GetValidationRules() =>
        _config.ExtendedValidation
            ? _customValidationRules.Concat(PipelineGraphValidator.ExtendedRules)
            : _customValidationRules.AsEnumerable();

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
        var profileDefaults = OptimizationProfileBehaviorRegistry.For(_config.OptimizationProfile).ResilienceDefaults;
        var resilience = BuildResilienceOptions(_config.ConfigureResilience, profileDefaults, "the pipeline");

        ImmutableDictionary<string, PipelineResilienceOptions>? nodeResilience = null;

        if (NodeState.ResilienceOverrides.Count > 0)
        {
            var builder = ImmutableDictionary.CreateBuilder<string, PipelineResilienceOptions>();

            foreach (var (nodeId, configure) in NodeState.ResilienceOverrides)
            {
                builder[nodeId] = BuildResilienceOptions(configure, resilience, $"node '{nodeId}'");
            }

            nodeResilience = builder.ToImmutable();
        }

        return new ErrorHandlingConfiguration
        {
            ResiliencePolicy = ConfigurationState.ResiliencePolicy,
            ResiliencePolicyType = ConfigurationState.ResiliencePolicyType,
            DeadLetterSink = ConfigurationState.DeadLetterSink,
            DeadLetterSinkType = ConfigurationState.DeadLetterSinkType,
            Resilience = resilience,
            NodeResilience = nodeResilience,
        };
    }

    private static PipelineResilienceOptions BuildResilienceOptions(
        Func<PipelineResilienceOptions, PipelineResilienceOptions>? configure,
        PipelineResilienceOptions baseline,
        string owner)
    {
        if (configure is null)
            return baseline;

        var options = configure(baseline)
                      ?? throw new InvalidOperationException($"The resilience configuration for {owner} returned null.");

        try
        {
            return options.Validate();
        }
        catch (ArgumentException ex)
        {
            throw new InvalidOperationException($"The resilience options for {owner} are invalid: {ex.Message}", ex);
        }
    }

    /// <summary>
    ///     Builds a LineageConfiguration from the current builder state.
    /// </summary>
    private LineageConfiguration BuildLineageConfiguration() =>
        new()
        {
            ItemLevelLineageEnabled = _config.ItemLevelLineageEnabled,
            LineageSink = ConfigurationState.LineageSink,
            LineageSinkType = ConfigurationState.LineageSinkType,
            PipelineLineageSink = ConfigurationState.PipelineLineageSink,
            PipelineLineageSinkType = ConfigurationState.PipelineLineageSinkType,
            LineageOptions = _config.LineageOptions,
        };

    /// <summary>
    ///     Builds an ExecutionOptionsConfiguration from the current builder state.
    /// </summary>
    private ExecutionOptionsConfiguration BuildExecutionOptionsConfiguration() =>
        new()
        {
            NodeExecutionAnnotations = NodeState.ExecutionAnnotations.Count > 0
                ? NodeState.ExecutionAnnotations.ToImmutableDictionary()
                : null,
            Visualizer = ConfigurationState.Visualizer,
        };

    private void WarnIfCompileTimeOptimizationProfileDiffers()
    {
        if (!TryResolveCompileTimeOptimizationProfile(out var compileTimeProfile, out var declaringAssembly))
            return;

        if (compileTimeProfile == _config.OptimizationProfile)
            return;

        // Warn once per assembly and runtime profile, so a graph built on every run (and every item of a composite)
        // does not flood the trace.
        if (!ProfileMismatchWarned.TryAdd((declaringAssembly, _config.OptimizationProfile), 0))
            return;

        Trace.TraceWarning(
            $"[NPipeline] Optimization profile mismatch detected: runtime profile '{_config.OptimizationProfile}' " +
            $"and compile-time analyzer profile '{compileTimeProfile}'. " +
            "Align PipelineBuilder.WithOptimizationProfile(...) and <NPipelineOptimizationProfile> to avoid analyzer/runtime drift.");
    }

    private static Assembly? EntryAssembly => Assembly.GetEntryAssembly();

    private bool TryResolveCompileTimeOptimizationProfile(out PipelineOptimizationProfile compileTimeProfile, out Assembly declaringAssembly)
    {
        foreach (var assembly in GetOptimizationProfileMetadataCandidates())
        {
            if (TryReadOptimizationProfileMetadata(assembly, out compileTimeProfile))
            {
                declaringAssembly = assembly;
                return true;
            }
        }

        compileTimeProfile = default;
        declaringAssembly = typeof(PipelineBuilder).Assembly;
        return false;
    }

    private IEnumerable<Assembly> GetOptimizationProfileMetadataCandidates()
    {
        var seen = new HashSet<Assembly>();

        var entryAssembly = EntryAssembly;

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
        if (CompileTimeProfileByAssembly.TryGetValue(assembly, out var cached))
        {
            compileTimeProfile = cached ?? default;
            return cached is not null;
        }

        var resolved = ReadOptimizationProfileMetadata(assembly);
        _ = CompileTimeProfileByAssembly.TryAdd(assembly, resolved);

        compileTimeProfile = resolved ?? default;
        return resolved is not null;
    }

    private static PipelineOptimizationProfile? ReadOptimizationProfileMetadata(Assembly assembly)
    {
        _ = Interlocked.Increment(ref AssemblyMetadataReadCount);

        foreach (var metadata in assembly.GetCustomAttributes<AssemblyMetadataAttribute>())
        {
            if (!string.Equals(metadata.Key, OptimizationProfileMetadataKey, StringComparison.Ordinal))
                continue;

            var value = metadata.Value?.Trim();

            if (string.IsNullOrWhiteSpace(value))
                continue;

            if (Enum.TryParse(value, true, out PipelineOptimizationProfile compileTimeProfile))
                return compileTimeProfile;
        }

        return null;
    }

    /// <summary>
    ///     Scans for composite nodes with <see cref="NodeDefinition.ChildDefinitionType" /> set,
    ///     builds their child pipeline graphs, and attaches them to the parent graph.
    /// </summary>
    /// <remarks>
    ///     Child graphs are structural only: the instances a child's <c>Define</c> created are disposed here and are
    ///     not carried on the child graph. A recursive composite is skipped with a trace warning rather than recursing
    ///     until the stack overflows.
    /// </remarks>
    private PipelineGraph BuildChildGraphs(PipelineGraph graph)
    {
        Dictionary<string, PipelineGraph>? childGraphs = null;
        var inProgress = _childGraphGuard ?? new HashSet<Type>();

        foreach (var node in graph.Nodes)
        {
            if (node.Kind != NodeKind.Composite || node.ChildDefinitionType is null)
                continue;

            try
            {
                if (!inProgress.Add(node.ChildDefinitionType))
                {
                    Trace.TraceWarning(
                        $"[NPipeline] Recursive composite definition '{node.ChildDefinitionType}' skipped when building child graphs.");
                    continue;
                }

                var childDef = (IPipelineDefinition)Activator.CreateInstance(node.ChildDefinitionType)!;

                // The child graph must be built with the same lineage module as its parent, otherwise its nodes get
                // adapters from a different module than the one that will run them.
                var childBuilder = new PipelineBuilder(Lineage, RegistrationPlanner) { _childGraphGuard = inProgress };

                // Child graph extraction is a build-time operation and uses an isolated default context.
                // Child Define() implementations should remain side-effect free and fast.
                var childContext = new PipelineContext();

                try
                {
                    childDef.Define(childBuilder, childContext);

                    if (childBuilder.TryBuild(out var childPipeline, out var childResult) && childPipeline is not null)
                    {
                        childGraphs ??= new Dictionary<string, PipelineGraph>();

                        // The graph is kept for introspection only. Its instances belong to the child builder and are
                        // disposed below, so a later run cannot be handed disposed objects.
                        childGraphs[node.Id] = childPipeline.Graph with
                        {
                            PreconfiguredNodeInstances = FrozenDictionary<string, INode>.Empty,
                        };
                    }
                    else
                    {
                        Trace.TraceWarning(
                            $"[NPipeline] Child graph for composite '{node.Id}' is invalid: {string.Join("; ", childResult.Errors)}");
                    }
                }
                finally
                {
                    _ = inProgress.Remove(node.ChildDefinitionType);
                    DisposeBuilderInstances(childBuilder);

                    try
                    {
                        childContext.DisposeAsync().AsTask().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceWarning($"[NPipeline] Disposing a child-graph context failed: {ex.Message}");
                    }
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

    /// <summary>
    ///     Disposes the instances a child builder created, once each. The builder disposables and the preconfigured
    ///     instances overlap, so both are visited with reference equality.
    /// </summary>
    private static void DisposeBuilderInstances(PipelineBuilder builder)
    {
        var seen = new HashSet<object>(ReferenceEqualityComparer.Instance);
        var candidates = builder.BuilderDisposables.Concat(builder.NodeState.PreconfiguredNodeInstances.Values);

        foreach (var candidate in candidates)
        {
            if (!seen.Add(candidate))
                continue;

            try
            {
                switch (candidate)
                {
                    case IAsyncDisposable asyncDisposable:
                        asyncDisposable.DisposeAsync().AsTask().GetAwaiter().GetResult();
                        break;
                    case IDisposable disposable:
                        disposable.Dispose();
                        break;
                }
            }
            catch (Exception ex)
            {
                Trace.TraceWarning($"[NPipeline] Disposing a child-graph instance failed: {ex.Message}");
            }
        }
    }
}
