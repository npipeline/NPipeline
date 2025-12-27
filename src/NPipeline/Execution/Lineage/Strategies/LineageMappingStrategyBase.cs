using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.Lineage;

namespace NPipeline.Execution.Lineage.Strategies;

/// <summary>
///     Base class providing shared lineage mapping helper methods for strategy implementations.
/// </summary>
/// <remarks>
///     Allocation notes:
///     - Materialization path uses lists sized to observed counts (or cap) and reuses local collections.
///     - Streaming path enumerates in lock-step minimizing buffering.
/// </remarks>
internal abstract class LineageMappingStrategyBase
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static ImmutableList<LineageHop> MaybeAppendHop(ImmutableList<LineageHop> existing, string nodeId, LineageOptions? opts)
    {
        var cap = opts != null && opts.MaxHopRecordsPerItem > 0
            ? opts.MaxHopRecordsPerItem
            : int.MaxValue;

        if (existing.Count >= cap)
            return existing;

        var truncated = existing.Count + 1 >= cap;
        var rec = new LineageHop(nodeId, HopDecisionFlags.Emitted, ObservedCardinality.One, null, null, null, truncated);
        return existing.Add(rec);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static ImmutableList<LineageHop> AppendHop(ImmutableList<LineageHop> existing, string nodeId, LineageOptions? opts, HopDecisionFlags outcome,
        ObservedCardinality cardinality, IReadOnlyList<int>? ancestry)
    {
        var cap = opts != null && opts.MaxHopRecordsPerItem > 0
            ? opts.MaxHopRecordsPerItem
            : int.MaxValue;

        if (existing.Count >= cap)
            return existing;

        var truncated = existing.Count + 1 >= cap;
        var rec = new LineageHop(nodeId, outcome, cardinality, ancestry?.Count, null, ancestry, truncated);
        return existing.Add(rec);
    }

    protected static IEnumerable<LineagePacket<TOut>> MapMaterialized<TIn, TOut>(List<LineagePacket<TIn>> inputs, List<TOut> outputs, string nodeId,
        TransformCardinality card, LineageOptions? opts, Type? mapperType, ILineageMapper? mapperInstance)
    {
        Dictionary<int, IReadOnlyList<int>>? recordsByOutput = null;

        if (mapperType is not null && mapperInstance is not null)
        {
            var mappingResult =
                mapperInstance.MapInputToOutputs(inputs.Cast<object>().ToList(), outputs.Cast<object>().ToList(), new LineageMappingContext(nodeId));

            recordsByOutput = mappingResult.Records.ToDictionary(r => r.OutputIndex, r => r.InputIndices);
        }

        var inCount = inputs.Count;
        var outCount = outputs.Count;

        if (card == TransformCardinality.OneToOne && inCount != outCount)
        {
            var missingInputs = new List<int>();
            var extraOutputs = new List<int>();
            var aggregated = new List<LineageAggregatedGroup>();

            if (recordsByOutput is null)
            {
                if (inCount > outCount)
                {
                    for (var mi = outCount; mi < inCount; mi++)
                    {
                        missingInputs.Add(mi);
                    }
                }

                if (outCount > inCount)
                {
                    for (var eo = inCount; eo < outCount; eo++)
                    {
                        extraOutputs.Add(eo);
                    }
                }
            }
            else
            {
                var inputSeen = new bool[inCount];

                foreach (var kv in recordsByOutput)
                {
                    var outputIdx = kv.Key;
                    var inputIdxs = kv.Value;

                    if (inputIdxs.Count == 0)
                        extraOutputs.Add(outputIdx);
                    else
                    {
                        foreach (var ii in inputIdxs)
                        {
                            if (ii >= 0 && ii < inCount)
                                inputSeen[ii] = true;
                            else
                                extraOutputs.Add(outputIdx);
                        }

                        if (inputIdxs.Count > 1)
                            aggregated.Add(new LineageAggregatedGroup(outputIdx, inputIdxs));
                    }
                }

                for (var i = 0; i < inCount; i++)
                {
                    if (!inputSeen[i])
                        missingInputs.Add(i);
                }
            }

            var ctx = new LineageMismatchContext(nodeId, inCount, outCount, missingInputs, extraOutputs, aggregated);
            opts?.OnMismatch?.Invoke(ctx);

            if (opts?.Strict == true)
                throw new InvalidOperationException($"Lineage cardinality mismatch in node '{nodeId}': inputs={inCount}, outputs={outCount}.");

            if (opts?.WarnOnMismatch == true)
            {
                Trace.TraceWarning(
                    $"[NPipeline.Lineage] Node {nodeId} 1:1 mismatch detected (in={inCount}, out={outCount}). MissingInputs={missingInputs.Count} ExtraOutputs={extraOutputs.Count} AggregatedOutputs={aggregated.Count}");
            }
        }

        for (var oi = 0; oi < outputs.Count; oi++)
        {
            var outputData = outputs[oi];
            LineagePacket<TIn>? inputPacket = null;
            IReadOnlyList<int>? ancestry = null;

            if (recordsByOutput is not null && recordsByOutput.TryGetValue(oi, out var inputIdxs))
            {
                ancestry = opts?.CaptureAncestryMapping == true
                    ? inputIdxs
                    : null;

                if (inputIdxs.Count > 0 && inputIdxs[0] < inputs.Count)
                    inputPacket = inputs[inputIdxs[0]];
            }
            else
            {
                if (oi < inputs.Count)
                    inputPacket = inputs[oi];
            }

            var outcome = ancestry is not null && ancestry.Count > 1
                ? HopDecisionFlags.Aggregated
                : HopDecisionFlags.Emitted;

            var cardinalityObserved = ancestry is null
                ? ObservedCardinality.Unknown
                : ancestry.Count == 0
                    ? ObservedCardinality.Zero
                    : ancestry.Count == 1
                        ? ObservedCardinality.One
                        : ObservedCardinality.Many;

            if (inputPacket is not null)
            {
                var hopRecords = inputPacket.LineageHops;

                if (inputPacket.Collect)
                    hopRecords = AppendHop(hopRecords, nodeId, opts, outcome, cardinalityObserved, ancestry);

                yield return new LineagePacket<TOut>(outputData, inputPacket.LineageId, inputPacket.TraversalPath.Add(nodeId))
                    { Collect = inputPacket.Collect, LineageHops = hopRecords };
            }
            else
                yield return new LineagePacket<TOut>(outputData, Guid.NewGuid(), ImmutableList.Create(nodeId));
        }
    }

    protected static async IAsyncEnumerable<LineagePacket<TOut>> PositionalStreamingMap<TIn, TOut>(IAsyncEnumerable<LineagePacket<TIn>> inAll,
        IAsyncEnumerable<TOut> outAll, string nodeId, TransformCardinality card, LineageOptions? opts, [EnumeratorCancellation] CancellationToken token)
    {
        await using var inputEnumerator = inAll.GetAsyncEnumerator(token);
        await using var outputEnumerator = outAll.GetAsyncEnumerator(token);
        var matchedInputCount = 0;
        var matchedOutputCount = 0;

        while (true)
        {
            var hasInput = await inputEnumerator.MoveNextAsync().ConfigureAwait(false);
            var hasOutput = await outputEnumerator.MoveNextAsync().ConfigureAwait(false);

            if (hasInput && hasOutput)
            {
                var inputPacket = inputEnumerator.Current;
                var outputData = outputEnumerator.Current;
                var hopRecords = inputPacket.LineageHops;

                if (inputPacket.Collect)
                    hopRecords = MaybeAppendHop(hopRecords, nodeId, opts);

                yield return new LineagePacket<TOut>(outputData, inputPacket.LineageId, inputPacket.TraversalPath.Add(nodeId))
                    { Collect = inputPacket.Collect, LineageHops = hopRecords };

                matchedInputCount++;
                matchedOutputCount++;
                continue;
            }

            var totalIn = matchedInputCount;
            var totalOut = matchedOutputCount;

            if (hasInput)
            {
                totalIn++;

                while (await inputEnumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    totalIn++;
                }
            }

            if (hasOutput)
            {
                totalOut++;

                while (await outputEnumerator.MoveNextAsync().ConfigureAwait(false))
                {
                    totalOut++;
                }
            }

            if (totalIn != totalOut && card == TransformCardinality.OneToOne)
            {
                var missingInputs = new List<int>();
                var extraOutputs = new List<int>();

                if (totalIn > totalOut)
                {
                    for (var mi = totalOut; mi < totalIn; mi++)
                    {
                        missingInputs.Add(mi);
                    }
                }
                else if (totalOut > totalIn)
                {
                    for (var eo = totalIn; eo < totalOut; eo++)
                    {
                        extraOutputs.Add(eo);
                    }
                }

                var ctx = new LineageMismatchContext(nodeId, totalIn, totalOut, missingInputs, extraOutputs, []);
                opts?.OnMismatch?.Invoke(ctx);

                if (opts?.Strict == true)
                {
                    throw new InvalidOperationException(
                        $"Lineage cardinality mismatch in node '{nodeId}': inputs={totalIn}, outputs={totalOut}. Declare intended cardinality with [TransformCardinality] or adjust transform for 1:1.");
                }

                if (opts?.WarnOnMismatch == true)
                {
                    Trace.TraceWarning(
                        $"[NPipeline.Lineage] Node {nodeId} 1:1 mismatch detected (in={totalIn}, out={totalOut}). MissingInputs={missingInputs.Count} ExtraOutputs={extraOutputs.Count}");
                }
            }

            break;
        }
    }
}
