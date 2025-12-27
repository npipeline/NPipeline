using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Lineage;

namespace NPipeline.Execution.Services;

/// <summary>
///     Implements interleave merge strategy for merging multiple data streams.
/// </summary>
public sealed class InterleaveMergeStrategy<T> : IMergeStrategy<T>
{
    /// <inheritdoc />
    public IDataPipe<T> Merge(IEnumerable<IDataPipe<T>> pipes, CancellationToken cancellationToken)
    {
        var typedPipes = pipes.ToList();

        // Detect lineage packet wrapping (IDataPipe<LineagePacket<TActual>>) erroneously typed as IDataPipe<T> via covariance misuse downstream.
        // If any pipe item type is LineagePacket<Something> and Something == typeof(T), adapt enumeration to yield inner Data.
        var adapted = typedPipes.Select(p => AdaptIfLineage(p));
        var mergedStream = InterleaveBounded(adapted.ToList(), null, cancellationToken);
        return new StreamingDataPipe<T>(mergedStream, "InterleavedStream");
    }

    private static IDataPipe<T> AdaptIfLineage(IDataPipe<T> pipe)
    {
        var innerType = pipe.GetType().GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDataPipe<>))?
            .GetGenericArguments()[0];

        if (innerType is { IsGenericType: true } && innerType.GetGenericTypeDefinition() == typeof(LineagePacket<>))
        {
            var payloadType = innerType.GetGenericArguments()[0];

            if (payloadType == typeof(T))

                // Wrap into a streaming pipe that projects packet.Data
                return new StreamingDataPipe<T>(Project(pipe), $"Projected_{pipe.StreamName}");
        }

        return pipe;

        static async IAsyncEnumerable<T> Project(IDataPipe<T> original, [EnumeratorCancellation] CancellationToken token = default)
        {
            await foreach (var o in original.WithCancellation(token).ConfigureAwait(false))
            {
                if (o is null)
                    continue;

                // When original is actually LineagePacket<T> but erased via cast, reflectively obtain Data prop.
                var t = o.GetType();

                if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(LineagePacket<>))
                {
                    var dataProp = t.GetProperty("Data")!;
                    var dataVal = dataProp.GetValue(o);

                    if (dataVal is T tv)
                        yield return tv;
                    else if (dataVal is not null)
                        yield return (T)dataVal; // last resort cast
                }
                else if (o is { } direct)
                    yield return direct;
            }
        }
    }

    private static async IAsyncEnumerable<T> InterleaveBounded(
        IReadOnlyList<IDataPipe<T>> dataPipes,
        int? capacity = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = capacity is { } c and > 0
            ? Channel.CreateBounded<T>(new BoundedChannelOptions(c)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
            })
            : Channel.CreateUnbounded<T>(new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

        var producerTasks = dataPipes
            .Select(dataPipe => Task.Run(async () =>
            {
                await foreach (var item in dataPipe.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    if (!await channel.Writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
                        break;

                    await channel.Writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
                }
            }, cancellationToken))
            .ToList();

        _ = Task.WhenAll(producerTasks).ContinueWith(
            t => channel.Writer.TryComplete(t.Exception),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            yield return item;
        }
    }
}
