using System.Collections.Concurrent;
using System.Linq.Expressions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Provides a service for wrapping data pipes with counting functionality.
/// </summary>
public sealed class CountingService : ICountingService
{
    private static readonly ConcurrentDictionary<Type, Func<IDataPipe, StatsCounter, PipelineContext, IDataPipe>> CountingWrapDelegateCache = new();

    /// <inheritdoc />
    public IDataPipe Wrap(IDataPipe pipe, PipelineContext context)
    {
        var counter = GetOrCreateCounter(context);
        var dataType = pipe.GetDataType();

        if (!CountingWrapDelegateCache.TryGetValue(dataType, out var del))
        {
            del = BuildCountingWrapDelegate(dataType);
            CountingWrapDelegateCache[dataType] = del;
        }

        return del(pipe, counter, context);
    }

    private static StatsCounter GetOrCreateCounter(PipelineContext context)
    {
        if (!context.Items.TryGetValue(PipelineContextKeys.TotalProcessedItems, out var statsObj) || statsObj is not StatsCounter counter)
        {
            counter = new StatsCounter();
            context.Items[PipelineContextKeys.TotalProcessedItems] = counter;
        }

        return counter;
    }

    private static Func<IDataPipe, StatsCounter, PipelineContext, IDataPipe> BuildCountingWrapDelegate(Type dataType)
    {
        var pipeParam = Expression.Parameter(typeof(IDataPipe), "pipe");
        var counterParam = Expression.Parameter(typeof(StatsCounter), "counter");
        var contextParam = Expression.Parameter(typeof(PipelineContext), "context");

        var typedIface = typeof(IDataPipe<>).MakeGenericType(dataType);
        var castPipe = Expression.Convert(pipeParam, typedIface);
        var wrapperType = typeof(CountingDataPipe<>).MakeGenericType(dataType);
        var ctor = wrapperType.GetConstructor([typedIface, typeof(StatsCounter), typeof(PipelineContext)])!;
        var newExpr = Expression.New(ctor, castPipe, counterParam, contextParam);
        var castResult = Expression.Convert(newExpr, typeof(IDataPipe));
        var lambda = Expression.Lambda<Func<IDataPipe, StatsCounter, PipelineContext, IDataPipe>>(castResult, pipeParam, counterParam, contextParam);
        return lambda.Compile();
    }
}
