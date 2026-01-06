using NPipeline.Configuration;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Composition;

/// <summary>
///     A transform node that executes a complete sub-pipeline for each input item.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
/// <typeparam name="TDefinition">The sub-pipeline definition type.</typeparam>
public sealed class CompositeTransformNode<TIn, TOut, TDefinition>
    : TransformNode<TIn, TOut>
    where TDefinition : IPipelineDefinition, new()
{
    private readonly CompositeContextConfiguration _contextConfiguration;
    private readonly IPipelineRunner _pipelineRunner;

    /// <summary>
    ///     Creates a new composite transform node.
    /// </summary>
    /// <param name="pipelineRunner">Runner for executing sub-pipelines.</param>
    /// <param name="contextConfiguration">Configuration for sub-pipeline context.</param>
    public CompositeTransformNode(
        IPipelineRunner pipelineRunner,
        CompositeContextConfiguration contextConfiguration)
    {
        _pipelineRunner = pipelineRunner ?? throw new ArgumentNullException(nameof(pipelineRunner));
        _contextConfiguration = contextConfiguration ?? CompositeContextConfiguration.Default;
    }

    /// <summary>
    ///     Executes the sub-pipeline for the given input item.
    /// </summary>
    public override async Task<TOut> ExecuteAsync(
        TIn item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Create isolated sub-pipeline context
        var subContext = CreateSubPipelineContext(context);

        // Store input item in sub-context
        subContext.Parameters[CompositeContextKeys.InputItem] = item!;

        // Execute sub-pipeline
        await _pipelineRunner.RunAsync<TDefinition>(subContext).ConfigureAwait(false);

        // Retrieve and return output item
        return RetrieveOutputItem(subContext);
    }

    private PipelineContext CreateSubPipelineContext(PipelineContext parentContext)
    {
        // Create isolated dictionaries
        var subParameters = _contextConfiguration.InheritParentParameters
            ? new Dictionary<string, object>(parentContext.Parameters)
            : [];

        var subItems = _contextConfiguration.InheritParentItems
            ? new Dictionary<string, object>(parentContext.Items)
            : [];

        var subProperties = _contextConfiguration.InheritParentProperties
            ? new Dictionary<string, object>(parentContext.Properties)
            : [];

        // Create new context with same factories and cancellation token
        var config = new PipelineContextConfiguration(
            subParameters,
            subItems,
            subProperties,
            CancellationToken: parentContext.CancellationToken,
            LoggerFactory: parentContext.LoggerFactory,
            Tracer: parentContext.Tracer,
            ErrorHandlerFactory: parentContext.ErrorHandlerFactory,
            LineageFactory: parentContext.LineageFactory,
            ObservabilityFactory: parentContext.ObservabilityFactory,
            RetryOptions: parentContext.RetryOptions);

        return new PipelineContext(config);
    }

    private TOut RetrieveOutputItem(PipelineContext subContext)
    {
        if (!subContext.Parameters.TryGetValue(CompositeContextKeys.OutputItem, out var outputItem))
        {
            throw new InvalidOperationException(
                "Sub-pipeline did not produce an output item. " +
                "Ensure PipelineOutputSink is properly configured.");
        }

        // Handle DBNull for nullable types
        if (outputItem is DBNull)
            outputItem = null;

        if (outputItem is TOut typedOutput)
            return typedOutput;

        // Handle null for nullable reference/value types
        if (outputItem == null)
        {
            // Only allow null if TOut is nullable
            if (default(TOut) == null || Nullable.GetUnderlyingType(typeof(TOut)) != null)
                return default!;

            throw new InvalidCastException(
                $"Sub-pipeline output is null, but {typeof(TOut)} is not nullable.");
        }

        throw new InvalidCastException(
            $"Sub-pipeline output type mismatch. Expected {typeof(TOut)}, got {outputItem.GetType()}.");
    }
}
