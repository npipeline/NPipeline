using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Observability;

namespace NPipeline.Execution;

/// <summary>
///     Wraps input streams to attribute MoveNextAsync await time to node-local input wait.
/// </summary>
public static class NodeTimingDataStreamWrapper
{
    private static readonly ConcurrentDictionary<Type, Func<IDataStream, IAutoObservabilityScope, IDataStream>> WrapInputWaitDelegates = new();

    /// <summary>
    ///     Wraps an untyped input stream with input-wait timing attribution.
    /// </summary>
    public static IDataStream WrapInputWait(IDataStream input, IAutoObservabilityScope scope)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(scope);

        if (ReferenceEquals(scope, NodeExecutionScopeRegistry.NullScope))
            return input;

        var dataType = input.GetDataType();
        var wrap = WrapInputWaitDelegates.GetOrAdd(dataType, static t => BuildWrapInputWaitDelegate(t));
        return wrap(input, scope);
    }

    /// <summary>
    ///     Wraps a typed input stream with input-wait timing attribution.
    /// </summary>
    public static IDataStream<T> WrapInputWait<T>(IDataStream<T> input, IAutoObservabilityScope scope)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(scope);

        if (ReferenceEquals(scope, NodeExecutionScopeRegistry.NullScope))
            return input;

        return new DataStream<T>(EnumerateWithInputWait(input, scope), input.StreamName);
    }

    private static Func<IDataStream, IAutoObservabilityScope, IDataStream> BuildWrapInputWaitDelegate(Type dataType)
    {
        var inputParam = Expression.Parameter(typeof(IDataStream), "input");
        var scopeParam = Expression.Parameter(typeof(IAutoObservabilityScope), "scope");

        var wrapMethod = typeof(NodeTimingDataStreamWrapper)
            .GetMethod(nameof(WrapInputWaitGeneric), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(dataType);

        var call = Expression.Call(wrapMethod, inputParam, scopeParam);
        return Expression.Lambda<Func<IDataStream, IAutoObservabilityScope, IDataStream>>(call, inputParam, scopeParam).Compile();
    }

    private static IDataStream WrapInputWaitGeneric<T>(IDataStream input, IAutoObservabilityScope scope) => WrapInputWait((IDataStream<T>)input, scope);

    private static async IAsyncEnumerable<T> EnumerateWithInputWait<T>(
        IDataStream<T> input,
        IAutoObservabilityScope scope,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
#pragma warning disable CA2007

        // CA2007 false positive: the enumerator comes from a ConfigureAwait(false) sequence, so its
        // MoveNextAsync and DisposeAsync already return configured awaitables - the analyzer only
        // recognises ConfigureAwait applied directly to the await using expression.
        await using var enumerator = input.WithCancellation(cancellationToken).ConfigureAwait(false).GetAsyncEnumerator();
#pragma warning restore CA2007

        while (true)
        {
            var waitStart = Stopwatch.GetTimestamp();
            bool hasNext;

            try
            {
                hasNext = await enumerator.MoveNextAsync();
            }
            finally
            {
                scope.AddInputWait(Stopwatch.GetElapsedTime(waitStart));
            }

            if (!hasNext)
                yield break;

            yield return enumerator.Current;
        }
    }
}
