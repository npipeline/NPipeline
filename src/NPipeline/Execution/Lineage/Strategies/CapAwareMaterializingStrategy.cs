using System.Diagnostics;
using System.Runtime.CompilerServices;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.Lineage;

namespace NPipeline.Execution.Lineage.Strategies;

internal sealed class CapAwareMaterializingStrategy<TIn, TOut> : LineageMappingStrategyBase, ILineageMappingStrategy<TIn, TOut>
{
    public static readonly CapAwareMaterializingStrategy<TIn, TOut> Instance = new();

    private CapAwareMaterializingStrategy()
    {
    }

    public async IAsyncEnumerable<LineagePacket<TOut>> MapAsync(IAsyncEnumerable<LineagePacket<TIn>> inputStream, IAsyncEnumerable<TOut> outputStream,
        string nodeId, TransformCardinality cardinality, LineageOptions? options, Type? lineageMapperType, ILineageMapper? mapperInstance,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var cap = options?.MaterializationCap;
        var policy = options?.OverflowPolicy ?? LineageOverflowPolicy.Degrade;

        if (cap is null || cap <= 0)
        {
            await foreach (var pkt in MaterializingStrategy<TIn, TOut>.Instance
                               .MapAsync(inputStream, outputStream, nodeId, cardinality, options, lineageMapperType, mapperInstance, ct))
            {
                yield return pkt;
            }

            yield break;
        }

        var inBuf = new List<LineagePacket<TIn>>(cap.Value);
        var outBuf = new List<TOut>(cap.Value);
        var inEnum = inputStream.GetAsyncEnumerator(ct);
        var outEnum = outputStream.GetAsyncEnumerator(ct);
        var inMore = await FillUpToCap(inEnum, inBuf, cap.Value).ConfigureAwait(false);
        var outMore = await FillUpToCap(outEnum, outBuf, cap.Value).ConfigureAwait(false);
        var overflow = inMore || outMore;

        if (!overflow)
        {
            await inEnum.DisposeAsync().ConfigureAwait(false);
            await outEnum.DisposeAsync().ConfigureAwait(false);

            foreach (var packet in MapMaterialized(inBuf, outBuf, nodeId, cardinality, options, lineageMapperType,
                         mapperInstance))
            {
                yield return packet;
            }

            yield break;
        }

        if (policy == LineageOverflowPolicy.Strict)
        {
            await inEnum.DisposeAsync().ConfigureAwait(false);
            await outEnum.DisposeAsync().ConfigureAwait(false);
            throw new InvalidOperationException($"[NPipeline.Lineage] Materialization cap exceeded for node '{nodeId}' (cap={cap}).");
        }

        options?.OnMismatch?.Invoke(new LineageMismatchContext(nodeId, inBuf.Count, outBuf.Count, [], [], []));

        if (options?.WarnOnMismatch == true)
            Trace.TraceWarning($"[NPipeline.Lineage] Node {nodeId} materialization cap exceeded (cap={cap}). Switching to positional streaming mapping.");

        if (policy == LineageOverflowPolicy.WarnContinue)
        {
            while (await inEnum.MoveNextAsync().ConfigureAwait(false))
            {
                inBuf.Add(inEnum.Current);
            }

            while (await outEnum.MoveNextAsync().ConfigureAwait(false))
            {
                outBuf.Add(outEnum.Current);
            }

            await inEnum.DisposeAsync().ConfigureAwait(false);
            await outEnum.DisposeAsync().ConfigureAwait(false);

            foreach (var packet in MapMaterialized(inBuf, outBuf, nodeId, cardinality, options, lineageMapperType,
                         mapperInstance))
            {
                yield return packet;
            }

            yield break;
        }

        // Degrade path: positional streaming, include buffered items + remainder
        await foreach (var packet in PositionalStreamingMap(InputAll(ct), OutputAll(ct), nodeId, cardinality, options, ct))
        {
            yield return packet;
        }

        yield break;

        async IAsyncEnumerable<LineagePacket<TIn>> InputAll([EnumeratorCancellation] CancellationToken token)
        {
            foreach (var it in inBuf)
            {
                yield return it;
            }

            if (inMore)
            {
                yield return inEnum.Current;

                while (await inEnum.MoveNextAsync().ConfigureAwait(false))
                {
                    yield return inEnum.Current;
                }
            }

            await inEnum.DisposeAsync().ConfigureAwait(false);
        }

        async IAsyncEnumerable<TOut> OutputAll([EnumeratorCancellation] CancellationToken token)
        {
            foreach (var ot in outBuf)
            {
                yield return ot;
            }

            if (outMore)
            {
                yield return outEnum.Current;

                while (await outEnum.MoveNextAsync().ConfigureAwait(false))
                {
                    yield return outEnum.Current;
                }
            }

            await outEnum.DisposeAsync().ConfigureAwait(false);
        }

        static async Task<bool> FillUpToCap<TItem>(IAsyncEnumerator<TItem> e, List<TItem> buf, int capValue)
        {
            while (buf.Count < capValue)
            {
                if (!await e.MoveNextAsync().ConfigureAwait(false))
                    return false;

                buf.Add(e.Current);
            }

            var hasMore = await e.MoveNextAsync().ConfigureAwait(false);
            return hasMore;
        }
    }
}
