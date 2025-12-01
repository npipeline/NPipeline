using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Lineage;

namespace NPipeline.Execution.Services;

/// <summary>
///     Provides services for managing item-level lineage throughout a pipeline execution.
/// </summary>
public sealed class LineageService : ILineageService
{
    private static readonly ConcurrentDictionary<Type, Func<IDataPipe, string, LineageOptions?, IDataPipe>> WrapSourceDelegates = new();

    private static readonly ConcurrentDictionary<Type, Func<object, string, LineageOptions?, HopDecisionFlags, CancellationToken, object>>
        WrapJoinOutputsDelegates = new();

    private static readonly ConcurrentDictionary<Type, Func<IEnumerable, object>> EnumerableToAsyncDelegates = new();

    /// <inheritdoc />
    public IDataPipe WrapSourceStream(IDataPipe sourcePipe, string nodeId, LineageOptions? options)
    {
        var dataType = sourcePipe.GetDataType();
        var del = WrapSourceDelegates.GetOrAdd(dataType, static t => BuildWrapSourceDelegate(t));
        return del(sourcePipe, nodeId, options);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<object> UnwrapLineageStream(IAsyncEnumerable<object?> source, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
        {
            if (item is null)
            {
                yield return null!;

                continue;
            }

            yield return UnwrapIfLineage(item);
        }
    }

    /// <inheritdoc />
    public IDataPipe WrapNodeOutput(IDataPipe output, string currentNodeId, LineageOptions? options, HopDecisionFlags outcome, CancellationToken ct = default)
    {
        var outType = output.GetDataType();

        if (output is IStreamingDataPipe)
        {
            var wrapDel = WrapJoinOutputsDelegates.GetOrAdd(outType, static t => BuildWrapJoinOutputsDelegate(t));

            // Ensure we pass an IAsyncEnumerable<outType> instance (not IAsyncEnumerable<object>) to the cached delegate.
            var rawAsync = output.ToAsyncEnumerable(ct); // returns IAsyncEnumerable<object>

            var castMethod =
                typeof(LineageService).GetMethod(nameof(CastAsyncEnumerable), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(outType);

            var typedAsync = castMethod.Invoke(null, [rawAsync])!; // IAsyncEnumerable<outType>
            var wrappedStream = wrapDel(typedAsync, currentNodeId, options, outcome, ct);
            var lineagePipeType = typeof(StreamingDataPipe<>).MakeGenericType(typeof(LineagePacket<>).MakeGenericType(outType));
            return (IDataPipe)Activator.CreateInstance(lineagePipeType, wrappedStream, $"LineageWrapped_{currentNodeId}")!;
        }

        // Handle non-streaming pipes (like InMemoryDataPipe)
        var enumerableData = ExtractDataFromPipe(output);

        var toAsyncDel = EnumerableToAsyncDelegates.GetOrAdd(outType, static t => BuildEnumerableToAsyncDelegate(t));
        var asyncEnumerable = toAsyncDel(enumerableData);

        var wrapDelegate = WrapJoinOutputsDelegates.GetOrAdd(outType, static t => BuildWrapJoinOutputsDelegate(t));
        var wrappedAsync = wrapDelegate(asyncEnumerable, currentNodeId, options, outcome, ct);
        var pipeType = typeof(StreamingDataPipe<>).MakeGenericType(typeof(LineagePacket<>).MakeGenericType(outType));
        return (IDataPipe)Activator.CreateInstance(pipeType, wrappedAsync, $"LineageWrapped_{currentNodeId}")!;
    }

    private static IAsyncEnumerable<T> CastAsyncEnumerable<T>(IAsyncEnumerable<object> source)
    {
        return CastImpl();

        async IAsyncEnumerable<T> CastImpl([EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var obj in source.WithCancellation(ct).ConfigureAwait(false))
            {
                if (obj is T t)
                    yield return t;
                else
                    yield return (T)obj!;
            }
        }
    }

    private static object UnwrapIfLineage(object item)
    {
        return item is ILineageEnvelope env
            ? env.Data!
            : item;
    }

    private static Func<IDataPipe, string, LineageOptions?, IDataPipe> BuildWrapSourceDelegate(Type dataType)
    {
        var method =
            typeof(LineageService).GetMethod(nameof(WrapSourceStreamGeneric), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(dataType);

        var pipeParam = Expression.Parameter(typeof(IDataPipe), "pipe");
        var nodeIdParam = Expression.Parameter(typeof(string), "nodeId");
        var optsParam = Expression.Parameter(typeof(LineageOptions), "options");
        var castPipe = Expression.Convert(pipeParam, typeof(IDataPipe<>).MakeGenericType(dataType));
        var call = Expression.Call(method, castPipe, nodeIdParam, optsParam);
        var castResult = Expression.Convert(call, typeof(IDataPipe));
        var lambda = Expression.Lambda<Func<IDataPipe, string, LineageOptions?, IDataPipe>>(castResult, pipeParam, nodeIdParam, optsParam);
        return lambda.Compile();
    }

    private static StreamingDataPipe<LineagePacket<T>> WrapSourceStreamGeneric<T>(IDataPipe<T> sourcePipe, string nodeId, LineageOptions? options)
    {
        return new StreamingDataPipe<LineagePacket<T>>(WrapStream(CancellationToken.None), $"LineageWrapped_{sourcePipe.StreamName}");

        async IAsyncEnumerable<LineagePacket<T>> WrapStream([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var item in sourcePipe.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var lineageId = Guid.NewGuid();
                var collect = ShouldCollect(lineageId, options);

                yield return new LineagePacket<T>(item, lineageId, ImmutableList.Create(nodeId))
                {
                    Collect = collect,
                };
            }
        }
    }

    private static bool ShouldCollect(Guid lineageId, LineageOptions? options)
    {
        if (options is null || options.SampleEvery <= 1)
            return true;

        var mod = options.SampleEvery;

        if (mod <= 0)
            return true;

        var hash = lineageId.GetHashCode() & int.MaxValue;
        return hash % mod == 0;
    }

    private static Func<object, string, LineageOptions?, HopDecisionFlags, CancellationToken, object> BuildWrapJoinOutputsDelegate(Type outType)
    {
        var method = typeof(LineageService).GetMethod(nameof(WrapJoinOutputsGeneric), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(outType);
        var outputParam = Expression.Parameter(typeof(object), "output");
        var nodeParam = Expression.Parameter(typeof(string), "nodeId");
        var optsParam = Expression.Parameter(typeof(LineageOptions), "options");
        var outcomeParam = Expression.Parameter(typeof(HopDecisionFlags), "outcome");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");
        var castOutput = Expression.Convert(outputParam, typeof(IAsyncEnumerable<>).MakeGenericType(outType));
        var call = Expression.Call(method, castOutput, nodeParam, optsParam, outcomeParam, ctParam);
        var castResult = Expression.Convert(call, typeof(object));

        var lambda = Expression.Lambda<Func<object, string, LineageOptions?, HopDecisionFlags, CancellationToken, object>>(castResult, outputParam, nodeParam,
            optsParam, outcomeParam, ctParam);

        return lambda.Compile();
    }

    private static async IAsyncEnumerable<LineagePacket<T>> WrapJoinOutputsGeneric<T>(
        IAsyncEnumerable<T> output,
        string currentNodeId,
        LineageOptions? options,
        HopDecisionFlags outcome,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in output.WithCancellation(ct).ConfigureAwait(false))
        {
            var lineageId = Guid.NewGuid();
            var collect = ShouldCollect(lineageId, options);
            var hopRecords = ImmutableList<LineageHop>.Empty;

            if (collect)
            {
                var cap = options != null && options.MaxHopRecordsPerItem > 0
                    ? options.MaxHopRecordsPerItem
                    : int.MaxValue;

                if (cap > 0)
                {
                    var seed = new LineageHop(
                        currentNodeId,
                        outcome,
                        ObservedCardinality.Unknown,
                        null,
                        null,
                        null,
                        false);

                    hopRecords = hopRecords.Add(seed);
                }
            }

            yield return new LineagePacket<T>(item!, lineageId, ImmutableList.Create(currentNodeId))
            {
                Collect = collect,
                LineageHops = hopRecords,
            };
        }
    }

    private static IEnumerable ExtractDataFromPipe(IDataPipe pipe)
    {
        // Handle different pipe types by extracting their underlying data
        return pipe switch
        {
            // InMemoryDataPipe has an Items property that contains the data
            var listPipe when listPipe.GetType().IsGenericType &&
                              listPipe.GetType().GetGenericTypeDefinition() == typeof(InMemoryDataPipe<>) =>
                listPipe.GetType().GetProperty("Items")?.GetValue(listPipe) as IEnumerable ??
                throw new InvalidOperationException("Failed to extract Items from InMemoryDataPipe"),

            // For any other non-streaming pipe, try to materialize its async enumerable
            _ => MaterializePipeData(pipe),
        };
    }

    private static List<object?> MaterializePipeData(IDataPipe pipe)
    {
        // For pipes that don't have a direct data extraction method,
        // materialize their async enumerable to a list
        var asyncEnum = pipe.ToAsyncEnumerable(CancellationToken.None);
        var list = new List<object?>();

        // Synchronously iterate through the async enumerable
        // This is not ideal but necessary for the lineage service
        var task = Task.Run(async () =>
        {
            await foreach (var item in asyncEnum.WithCancellation(CancellationToken.None))
            {
                list.Add(item);
            }
        });

        // Wait for the task to complete (this is a blocking operation)
        task.GetAwaiter().GetResult();

        return list;
    }

    private static Func<IEnumerable, object> BuildEnumerableToAsyncDelegate(Type t)
    {
        var method = typeof(LineageService).GetMethod(nameof(EnumerableToAsync), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(t);
        var srcParam = Expression.Parameter(typeof(IEnumerable), "src");
        var call = Expression.Call(method, srcParam);
        var castResult = Expression.Convert(call, typeof(object));
        var lambda = Expression.Lambda<Func<IEnumerable, object>>(castResult, srcParam);
        return lambda.Compile();
    }

    private static async IAsyncEnumerable<T> EnumerableToAsync<T>(
        IEnumerable source,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var item in source)
        {
            ct.ThrowIfCancellationRequested();

            if (item is T t)
                yield return t;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }
}
