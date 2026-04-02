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
    where TDefinition : IPipelineDefinition
{
    private readonly CompositeContextConfiguration _contextConfiguration;
    private readonly bool _fallbackToParameterlessWhenServiceMissing;
    private readonly IPipelineRunner _pipelineRunner;
    private readonly IServiceProvider? _serviceProvider;

    /// <summary>
    ///     Creates a new composite transform node.
    /// </summary>
    /// <param name="pipelineRunner">Runner for executing sub-pipelines.</param>
    /// <param name="contextConfiguration">Configuration for sub-pipeline context.</param>
    /// <param name="serviceProvider">Optional service provider for resolving DI-managed child definitions.</param>
    /// <param name="fallbackToParameterlessWhenServiceMissing">
    ///     If true and <paramref name="serviceProvider" /> cannot resolve <typeparamref name="TDefinition" />,
    ///     attempts to create the definition using a parameterless constructor.
    /// </param>
    public CompositeTransformNode(
        IPipelineRunner pipelineRunner,
        CompositeContextConfiguration contextConfiguration,
        IServiceProvider? serviceProvider = null,
        bool fallbackToParameterlessWhenServiceMissing = false)
    {
        _pipelineRunner = pipelineRunner ?? throw new ArgumentNullException(nameof(pipelineRunner));
        _contextConfiguration = contextConfiguration ?? CompositeContextConfiguration.Default;
        _fallbackToParameterlessWhenServiceMissing = fallbackToParameterlessWhenServiceMissing;
        _serviceProvider = serviceProvider;
    }

    /// <summary>
    ///     Executes the sub-pipeline for the given input item.
    /// </summary>
    public override async Task<TOut> TransformAsync(
        TIn item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Create isolated sub-pipeline context
        var subContext = CreateSubPipelineContext(context);
        subContext.PipelineName = typeof(TDefinition).Name;

        // Store input item in sub-context
        subContext.Parameters[CompositeContextKeys.InputItem] = item is null ? DBNull.Value : item;

        var definition = ResolveDefinition();
        await _pipelineRunner.RunAsync(definition, subContext, cancellationToken).ConfigureAwait(false);

        // Retrieve and return output item
        return RetrieveOutputItem(subContext);
    }

    private IPipelineDefinition ResolveDefinition()
    {
        if (_serviceProvider is not null)
        {
            var resolved = _serviceProvider.GetService(typeof(TDefinition));

            if (resolved is IPipelineDefinition definition)
                return definition;

            if (!_fallbackToParameterlessWhenServiceMissing)
            {
                throw new InvalidOperationException(
                    $"Unable to resolve child pipeline definition '{typeof(TDefinition).FullName}' from IServiceProvider. " +
                    "Register the definition type or enable fallbackToParameterlessWhenServiceMissing.");
            }
        }

        return CreateDefinitionUsingParameterlessConstructor();
    }

    private static IPipelineDefinition CreateDefinitionUsingParameterlessConstructor()
    {
        try
        {
            return Activator.CreateInstance<TDefinition>();
        }
        catch (Exception ex) when (ex is MissingMethodException or MemberAccessException)
        {
            throw new InvalidOperationException(
                $"Unable to create child pipeline definition '{typeof(TDefinition).FullName}'. " +
                "Ensure it has a public parameterless constructor or provide it via IServiceProvider.",
                ex);
        }
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

        var subContext = new PipelineContext(config);

        if (_contextConfiguration.InheritRunIdentity)
            subContext.RunId = parentContext.RunId;

        // Inherit observability and lineage concerns based on configuration
        if (_contextConfiguration.InheritExecutionObserver)
            subContext.ExecutionObserver = parentContext.ExecutionObserver;

        if (_contextConfiguration.InheritLineageSink)
        {
            subContext.LineageSink = parentContext.LineageSink;
            subContext.PipelineLineageSink = parentContext.PipelineLineageSink;
        }

        if (_contextConfiguration.InheritDeadLetterDecorator)
        {
            subContext.DeadLetterSink = parentContext.DeadLetterSink;

            if (parentContext.Properties.TryGetValue(PipelineContextKeys.DeadLetterSinkDecorator, out var decorator))
                subContext.Properties[PipelineContextKeys.DeadLetterSinkDecorator] = decorator;
        }

        return subContext;
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
