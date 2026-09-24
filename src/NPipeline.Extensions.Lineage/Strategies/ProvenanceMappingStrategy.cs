using System.Diagnostics;
using System.Runtime.CompilerServices;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.Execution.Lineage;

namespace NPipeline.Lineage;

/// <summary>
///     Maps each output to the input item that produced it, as reported by the node's execution strategy (or, for a
///     stream transform, by the node), instead of by position.
/// </summary>
/// <remarks>
///     <para>
///         The strategy reports each output's input index just before it yields the output, so when an output arrives
///         here its report is already queued. Pairing by index is right whatever the node does between input and
///         output: skip or dead-letter an item, drop it under backpressure, filter it, expand it into several outputs,
///         or complete items out of order.
///     </para>
///     <para>
///         An item that ends without an output has no packet to carry its lineage to a sink node, so its terminal hop
///         is recorded here, directly to the lineage sink, with the reason it ended.
///     </para>
///     <para>
///         Only the input packets between the oldest unfinished item and the newest item read are held. A finished
///         item's lineage state is removed from the node's registry, so neither grows with the length of the stream.
///     </para>
/// </remarks>
internal sealed class ProvenanceMappingStrategy<TIn, TOut> : LineageMappingStrategyBase, ILineageMappingStrategy<TIn, TOut>
{
    public static readonly ProvenanceMappingStrategy<TIn, TOut> Instance = new();

    private ProvenanceMappingStrategy()
    {
    }

    public async IAsyncEnumerable<LineagePacket<TOut>> MapAsync(IAsyncEnumerable<LineagePacket<TIn>> inputStream, IAsyncEnumerable<TOut> outputStream,
        string nodeId, Guid pipelineId, string? pipelineName, TransformCardinality cardinality, LineageOptions? options, Type? lineageMapperType,
        ILineageMapper? mapperInstance,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var lineage = LineageNodeOutcomeRegistry.GetWriter(pipelineId, nodeId);
        var inputs = new PendingInputs(inputStream.GetAsyncEnumerator(ct));
        var warnedUnknown = false;

        await using (inputs.ConfigureAwait(false))
        {
            await foreach (var output in outputStream.WithCancellation(ct).ConfigureAwait(false))
            {
                LineageProvenance? source = null;

                // Items that ended without an output since the previous output are reported ahead of this one.
                while (lineage.TryTakeProvenance(out var report))
                {
                    if (report.Kind == LineageProvenanceKind.Done)
                    {
                        await EndAsync(report, inputs, lineage, nodeId, pipelineId, pipelineName, options, ct).ConfigureAwait(false);
                        continue;
                    }

                    source = report;
                    break;
                }

                var packet = source is { } found
                    ? await inputs.GetAsync(found.InputIndex).ConfigureAwait(false)
                    : null;

                if (packet is null)
                {
                    // No report, or one for an item already finished (an unordered restart can deliver an item twice).
                    if (!warnedUnknown)
                    {
                        warnedUnknown = true;
                        Trace.TraceWarning($"[NPipeline.Lineage] Node {nodeId} yielded an output whose input is unknown; it starts fresh lineage.");
                    }

                    yield return MintPacket(output, nodeId, pipelineId, pipelineName, options);
                    continue;
                }

                var index = source!.Value.InputIndex;
                var isOnlyOutput = source.Value.Kind == LineageProvenanceKind.Output;
                var traversalPath = packet.TraversalPath.Add(QualifyNodeId(nodeId, pipelineId));
                var lineageRecords = packet.LineageRecords;

                if (packet.Collect)
                {
                    var (outcome, retryCount) = ResolveRecordedOutcome(pipelineId, nodeId, index, LineageOutcomeReason.Emitted);

                    lineageRecords = MaybeAppendHop(lineageRecords, packet.CorrelationId, traversalPath, nodeId, pipelineId, pipelineName, options,
                        isOnlyOutput ? 1 : null, packet.Data, output, outcome, retryCount);
                }

                if (isOnlyOutput)
                    Finish(index, inputs, lineage);

                yield return new LineagePacket<TOut>(output, packet.CorrelationId, traversalPath)
                { Collect = packet.Collect, LineageRecords = lineageRecords };
            }

            // Items that ended after the last output. Every report is made before the node's output completes.
            while (lineage.TryTakeProvenance(out var report))
            {
                if (report.Kind == LineageProvenanceKind.Done)
                    await EndAsync(report, inputs, lineage, nodeId, pipelineId, pipelineName, options, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    ///     Finishes an item. One that produced no output has its terminal hop recorded to the lineage sink.
    /// </summary>
    private static async ValueTask EndAsync(LineageProvenance report, PendingInputs inputs, LineageNodeOutcomeWriter lineage, string nodeId,
        Guid pipelineId, string? pipelineName, LineageOptions? options, CancellationToken ct)
    {
        var packet = await inputs.GetAsync(report.InputIndex).ConfigureAwait(false);

        // Null when the item was finished already: a replay after a restart can report it again.
        if (packet is null)
            return;

        var recorded = report.Outcome != LineageOutcomeReason.Emitted
                       && (report.Outcome != LineageOutcomeReason.DroppedByBackpressure || options?.EmitBackpressureDropRecords != false);

        if (recorded && packet.Collect && lineage.Sink is { } sink)
        {
            var (outcome, retryCount) = ResolveRecordedOutcome(pipelineId, nodeId, report.InputIndex, report.Outcome);
            var traversalPath = packet.TraversalPath.Add(QualifyNodeId(nodeId, pipelineId));

            var records = AppendTerminalHop(packet.LineageRecords, packet.CorrelationId, traversalPath, nodeId, pipelineId, pipelineName, options,
                outcome, packet.Data, retryCount);

            // The upstream hops rode on the packet, and no sink node will see it, so they are recorded here too.
            var first = options?.EmitIntermediateNodeRecords == false
                ? records.Length - 1
                : 0;

            for (var i = first; i < records.Length; i++)
            {
                await sink.RecordAsync(records[i], ct).ConfigureAwait(false);
            }
        }

        Finish(report.InputIndex, inputs, lineage);
    }

    private static void Finish(long index, PendingInputs inputs, LineageNodeOutcomeWriter lineage)
    {
        inputs.Remove(index);
        lineage.Forget(index);
    }

    /// <summary>
    ///     The node's input packets, read on demand in input order and held until their item is finished.
    /// </summary>
    private sealed class PendingInputs(IAsyncEnumerator<LineagePacket<TIn>> enumerator) : IAsyncDisposable
    {
        private readonly Dictionary<long, LineagePacket<TIn>> _pending = [];
        private bool _ended;
        private long _read;

        public ValueTask DisposeAsync()
        {
            return enumerator.DisposeAsync();
        }

        /// <summary>
        ///     Gets the packet of the item at <paramref name="index" />, or null when that item is finished already or the
        ///     input ended before it.
        /// </summary>
        /// <remarks>
        ///     The adapter writes an item's packet before it hands the item to the node, so a packet for an item the node
        ///     has reported on is always there to read.
        /// </remarks>
        public async ValueTask<LineagePacket<TIn>?> GetAsync(long index)
        {
            if (_pending.TryGetValue(index, out var packet))
                return packet;

            while (_read <= index && !_ended)
            {
                if (await enumerator.MoveNextAsync().ConfigureAwait(false))
                    _pending[_read++] = enumerator.Current;
                else
                    _ended = true;
            }

            return _pending.GetValueOrDefault(index);
        }

        public void Remove(long index)
        {
            _ = _pending.Remove(index);
        }
    }
}
