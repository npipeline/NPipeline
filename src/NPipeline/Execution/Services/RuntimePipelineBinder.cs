using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Default runtime binder for pipeline runs.
/// </summary>
public sealed class RuntimePipelineBinder : IRuntimePipelineBinder
{
    /// <summary>
    ///     Shared singleton instance for the stateless runtime binder.
    /// </summary>
    public static RuntimePipelineBinder Instance { get; } = new();

    /// <inheritdoc />
    public Task<RuntimePipelineBindingResult> BindAsync(PipelineGraph graph, PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(context);

        var overriddenGraph = ApplyRuntimeItemLevelLineageOverride(graph, context);
        overriddenGraph = ApplyRuntimeLineageOptionsOverride(overriddenGraph, context);

        var errorHandler = ResolvePipelineErrorHandler(overriddenGraph, context.ErrorHandlerFactory);
        var deadLetterSink = ResolveDeadLetterSink(overriddenGraph, context.ErrorHandlerFactory);
        deadLetterSink = ApplyDeadLetterSinkDecorator(context, deadLetterSink);

        var lineageSink = overriddenGraph.Lineage.ItemLevelLineageEnabled
            ? ResolveLineageSink(overriddenGraph, context.LineageFactory, context)
            : null;

        lineageSink = ApplyLineageSinkDecorator(context, lineageSink);

        var pipelineLineageSink = ResolvePipelineLineageSink(overriddenGraph, context.LineageFactory, context);

        return Task.FromResult(new RuntimePipelineBindingResult(
            overriddenGraph,
            errorHandler,
            deadLetterSink,
            lineageSink,
            pipelineLineageSink));
    }

    private static PipelineGraph ApplyRuntimeLineageOptionsOverride(PipelineGraph graph, PipelineContext context)
    {
        if (!context.Properties.TryGetValue(PipelineContextKeys.LineageOptionsOverride, out var overrideObj) || overrideObj is null)
            return graph;

        var resolved = overrideObj switch
        {
            LineageOptions direct => direct,
            Func<LineageOptions?, LineageOptions?> factory => factory(graph.Lineage.LineageOptions),
            _ => graph.Lineage.LineageOptions,
        };

        return graph with
        {
            Lineage = graph.Lineage with
            {
                LineageOptions = resolved,
            },
        };
    }

    private static PipelineGraph ApplyRuntimeItemLevelLineageOverride(PipelineGraph graph, PipelineContext context)
    {
        if (!context.Properties.TryGetValue(PipelineContextKeys.ItemLevelLineageEnabledOverride, out var overrideObj) ||
            overrideObj is not bool enabled)
            return graph;

        var resolvedOptions = graph.Lineage.LineageOptions;

        if (enabled && resolvedOptions is null)
        {
            // Mirror PipelineBuilder.EnableItemLevelLineage() defaults for runtime enablement.
            resolvedOptions = LineageOptions.CompleteLineage;
        }

        return graph with
        {
            Lineage = graph.Lineage with
            {
                ItemLevelLineageEnabled = enabled,
                LineageOptions = resolvedOptions,
            },
        };
    }

    private static IDeadLetterSink? ApplyDeadLetterSinkDecorator(PipelineContext context, IDeadLetterSink? deadLetterSink)
    {
        if (context.Properties.TryGetValue(PipelineContextKeys.DeadLetterSinkDecorator, out var decoratorObj) &&
            decoratorObj is Func<IDeadLetterSink?, IDeadLetterSink?> decorator)
            return decorator(deadLetterSink);

        return deadLetterSink;
    }

    private static ILineageSink? ApplyLineageSinkDecorator(PipelineContext context, ILineageSink? lineageSink)
    {
        if (context.Properties.TryGetValue(PipelineContextKeys.LineageSinkDecorator, out var decoratorObj) &&
            decoratorObj is Func<ILineageSink?, ILineageSink?> decorator)
            return decorator(lineageSink);

        return lineageSink;
    }

    private static IPipelineErrorHandler? ResolvePipelineErrorHandler(PipelineGraph graph, IErrorHandlerFactory errorHandlerFactory)
    {
        if (graph.ErrorHandling.PipelineErrorHandler is not null)
            return graph.ErrorHandling.PipelineErrorHandler;

        if (graph.ErrorHandling.PipelineErrorHandlerType is not null)
            return errorHandlerFactory.CreateErrorHandler(graph.ErrorHandling.PipelineErrorHandlerType);

        return null;
    }

    private static IDeadLetterSink? ResolveDeadLetterSink(PipelineGraph graph, IErrorHandlerFactory errorHandlerFactory)
    {
        if (graph.ErrorHandling.DeadLetterSink is not null)
            return graph.ErrorHandling.DeadLetterSink;

        if (graph.ErrorHandling.DeadLetterSinkType is not null)
            return errorHandlerFactory.CreateDeadLetterSink(graph.ErrorHandling.DeadLetterSinkType);

        return null;
    }

    private static ILineageSink? ResolveLineageSink(PipelineGraph graph, ILineageFactory lineageFactory, PipelineContext context)
    {
        if (graph.Lineage.LineageSink is not null)
            return graph.Lineage.LineageSink;

        if (graph.Lineage.LineageSinkType is not null)
            return lineageFactory.CreateLineageSink(graph.Lineage.LineageSinkType);

        if (context.LineageSink is not null)
            return context.LineageSink;

        return null;
    }

    private static IPipelineLineageSink? ResolvePipelineLineageSink(PipelineGraph graph, ILineageFactory lineageFactory, PipelineContext context)
    {
        if (graph.Lineage.PipelineLineageSink is not null)
            return graph.Lineage.PipelineLineageSink;

        if (graph.Lineage.PipelineLineageSinkType is not null)
            return lineageFactory.CreatePipelineLineageSink(graph.Lineage.PipelineLineageSinkType);

        if (context.PipelineLineageSink is not null)
            return context.PipelineLineageSink;

        // Provider-based default (no reflection):
        // When item-level lineage is enabled and no explicit sink is configured,
        // attempt to resolve a provider (supplied by optional packages like NPipeline.Lineage)
        // and let it create the default sink.
        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var provider = lineageFactory.ResolvePipelineLineageSinkProvider();
            var provided = provider?.Create(context);

            if (provided is not null)
                return provided;
        }

        return null;
    }
}