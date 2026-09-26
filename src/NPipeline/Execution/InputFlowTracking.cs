using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;

namespace NPipeline.Execution;

/// <summary>
///     Whether a node has started reading its input during its current execution. Node retry (L3) executes a node
///     again only while this is unset: once a read has started, a forward-only input cannot be read again from the
///     start (the read may already have run the upstream chain), so a second execution would lose or duplicate items.
/// </summary>
internal sealed class InputFlow
{
    private volatile bool _flowed;

    public bool HasFlowed => _flowed;

    public void MarkFlowed()
    {
        _flowed = true;
    }

    public void Reset()
    {
        _flowed = false;
    }
}

/// <summary>
///     Wraps a node's input so that the start of its first read marks the node's <see cref="InputFlow" />.
/// </summary>
internal static class InputFlowTracking
{
    private static readonly ConcurrentDictionary<Type, Func<IDataStream, InputFlow, IDataStream>> Wrappers = new();

    private static readonly MethodInfo WrapGenericMethod =
        typeof(InputFlowTracking).GetMethod(nameof(WrapGeneric), BindingFlags.NonPublic | BindingFlags.Static)!;

    public static IDataStream Wrap(IDataStream input, InputFlow flow)
    {
        var wrap = Wrappers.GetOrAdd(input.GetDataType(),
            static type => WrapGenericMethod.MakeGenericMethod(type).CreateDelegate<Func<IDataStream, InputFlow, IDataStream>>());

        return wrap(input, flow);
    }

    private static IDataStream WrapGeneric<T>(IDataStream input, InputFlow flow)
    {
        var typed = (IDataStream<T>)input;
        return new DataStream<T>(Enumerate(typed, flow), typed.StreamName);
    }

    private static async IAsyncEnumerable<T> Enumerate<T>(IDataStream<T> input, InputFlow flow,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // The input is read as soon as the consumer asks for its first item, which is what runs this body. Mark that
        // before the first item is yielded: a failure thrown by the upstream enumerator (a source timeout, or an
        // upstream transform with no retry of its own) is then attributed to this node's already-consumed input,
        // rather than letting node retry execute this node again and re-run the whole upstream chain.
        flow.MarkFlowed();

        await foreach (var item in input.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }
}
