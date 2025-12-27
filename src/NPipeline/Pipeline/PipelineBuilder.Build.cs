using System.Collections.Frozen;
using System.Collections.Immutable;
using System.Diagnostics;
using NPipeline.Configuration;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Graph.Validation;

namespace NPipeline.Pipeline;

/// <summary>
///     Pipeline build and validation methods for PipelineBuilder.
/// </summary>
public sealed partial class PipelineBuilder
{
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

        if (ConfigurationState.GlobalExecutionObserver is not null)
            NodeState.ExecutionAnnotations[ExecutionAnnotationKeys.GlobalExecutionObserver] = ConfigurationState.GlobalExecutionObserver;

        // Build configuration objects from builder state
        var (errorHandlingConfig, lineageConfig, executionConfig) = BuildConfigurations();

        // Create the immutable nodes list
        var nodesList = NodeState.Nodes.Values.ToImmutableList();

        // Create a cached frozen dictionary for O(1) node lookups during execution
        var nodeDefinitionMap = nodesList.ToFrozenDictionary(n => n.Id);

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(nodesList)
            .WithEdges(ConnectionState.Edges.ToImmutableList())
            .WithPreconfiguredNodeInstances(NodeState.PreconfiguredNodeInstances.ToImmutableDictionary())
            .WithNodeDefinitionMap(nodeDefinitionMap)
            .WithErrorHandlingConfiguration(errorHandlingConfig)
            .WithLineageConfiguration(lineageConfig)
            .WithExecutionOptionsConfiguration(executionConfig)
            .Build();

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

        if (ConfigurationState.GlobalExecutionObserver is not null)
            NodeState.ExecutionAnnotations[ExecutionAnnotationKeys.GlobalExecutionObserver] = ConfigurationState.GlobalExecutionObserver;

        // Build configuration objects from builder state
        var (errorHandlingConfig, lineageConfig, executionConfig) = BuildConfigurations();

        // Create the immutable nodes list
        var nodesList = NodeState.Nodes.Values.ToImmutableList();

        // Create a cached frozen dictionary for O(1) node lookups during execution
        var nodeDefinitionMap = nodesList.ToFrozenDictionary(n => n.Id);

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(nodesList)
            .WithEdges(ConnectionState.Edges.ToImmutableList())
            .WithPreconfiguredNodeInstances(NodeState.PreconfiguredNodeInstances.ToImmutableDictionary())
            .WithNodeDefinitionMap(nodeDefinitionMap)
            .WithErrorHandlingConfiguration(errorHandlingConfig)
            .WithLineageConfiguration(lineageConfig)
            .WithExecutionOptionsConfiguration(executionConfig)
            .Build();

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
        var overrideDict = NodeState.RetryOverrides.Count > 0
            ? NodeState.RetryOverrides.ToImmutableDictionary()
            : null;

        return new ErrorHandlingConfiguration
        {
            PipelineErrorHandler = ConfigurationState.PipelineErrorHandler,
            DeadLetterSink = ConfigurationState.DeadLetterSink,
            PipelineErrorHandlerType = ConfigurationState.PipelineErrorHandlerType,
            DeadLetterSinkType = ConfigurationState.DeadLetterSinkType,
            RetryOptions = _config.RetryOptions,
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
}
