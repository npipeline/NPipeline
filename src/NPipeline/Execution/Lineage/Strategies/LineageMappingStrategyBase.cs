using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.Execution.Lineage;
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
    private static readonly JsonSerializerOptions SnapshotSerializerOptions = new(JsonSerializerDefaults.Web)
    {
        ReferenceHandler = System.Text.Json.Serialization.ReferenceHandler.IgnoreCycles,
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static object? SnapshotValue(object? value, LineageOptions? opts)
    {
        if (opts?.CaptureHopSnapshots != true || value is null)
            return null;

        try
        {
            // Serialize/deserialize to produce an immutable-by-value snapshot of the object graph.
            return JsonSerializer.SerializeToElement(value, SnapshotSerializerOptions);
        }
        catch
        {
            // Best-effort snapshot: keep lineage flow robust even for non-serializable payloads.
            return value.ToString();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static ImmutableList<LineageHop> MaybeAppendHop(ImmutableList<LineageHop> existing, string nodeId, Guid pipelineId,
        string? pipelineName, LineageOptions? opts, int? outputEmissionCount, object? inputSnapshot, object? outputSnapshot,
        HopDecisionFlags outcome = HopDecisionFlags.Emitted, int? retryCount = null)
    {
        var cap = opts != null && opts.MaxHopRecordsPerItem > 0
            ? opts.MaxHopRecordsPerItem
            : int.MaxValue;

        if (existing.Count >= cap)
            return existing;

        var truncated = existing.Count + 1 >= cap;
        var rec = new LineageHop(nodeId, outcome, ObservedCardinality.One, null, outputEmissionCount, null, truncated, pipelineId,
            SnapshotValue(inputSnapshot, opts), SnapshotValue(outputSnapshot, opts), pipelineName, retryCount);
        return existing.Add(rec);
    }

    /// <summary>
    ///     Returns a traversal-path segment qualified with the pipeline identity.
    ///     This matches the scheme used by <see cref="NPipeline.Lineage.LineageCollector" /> when recording hops with a non-empty pipeline id.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static string QualifyNodeId(string nodeId, Guid pipelineId)
        => $"{pipelineId:N}::{nodeId}";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static ImmutableList<LineageHop> AppendHop(ImmutableList<LineageHop> existing, string nodeId, Guid pipelineId,
        string? pipelineName, LineageOptions? opts, HopDecisionFlags outcome, ObservedCardinality cardinality, IReadOnlyList<int>? ancestry,
        int? outputEmissionCount, object? inputSnapshot, object? outputSnapshot, int? retryCount = null)
    {
        var cap = opts != null && opts.MaxHopRecordsPerItem > 0
            ? opts.MaxHopRecordsPerItem
            : int.MaxValue;

        if (existing.Count >= cap)
            return existing;

        var truncated = existing.Count + 1 >= cap;
        var rec = new LineageHop(nodeId, outcome, cardinality, ancestry?.Count, outputEmissionCount, ancestry, truncated, pipelineId,
            SnapshotValue(inputSnapshot, opts), SnapshotValue(outputSnapshot, opts), pipelineName, retryCount);
        return existing.Add(rec);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static HopDecisionFlags WithoutRetry(HopDecisionFlags flags)
        => flags & ~HopDecisionFlags.Retried;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static (HopDecisionFlags Outcome, int? RetryCount) ResolveRecordedOutcome(Guid pipelineId, string nodeId, int inputIndex,
        HopDecisionFlags baseOutcome)
    {
        if (!LineageNodeOutcomeRegistry.TryGet(pipelineId, nodeId, inputIndex, out var recorded))
            return (baseOutcome, null);

        var retryCount = recorded.RetryCount > 0
            ? recorded.RetryCount
            : (int?)null;

        var outcome = baseOutcome | WithoutRetry(recorded.OutcomeFlags);

        if ((recorded.OutcomeFlags & HopDecisionFlags.Retried) != 0 || recorded.RetryCount > 0)
            outcome |= HopDecisionFlags.Retried;

        return (outcome, retryCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (HopDecisionFlags Outcome, int? RetryCount) ResolveRecordedOutcome(Guid pipelineId, string nodeId,
        IReadOnlyList<int>? contributorIndices, HopDecisionFlags baseOutcome)
    {
        if (contributorIndices is null || contributorIndices.Count == 0)
            return (baseOutcome, null);

        var outcome = baseOutcome;
        var hasRetry = false;
        var maxRetryCount = 0;

        foreach (var contributorIndex in contributorIndices)
        {
            if (!LineageNodeOutcomeRegistry.TryGet(pipelineId, nodeId, contributorIndex, out var recorded))
                continue;

            outcome |= WithoutRetry(recorded.OutcomeFlags);

            if ((recorded.OutcomeFlags & HopDecisionFlags.Retried) != 0 || recorded.RetryCount > 0)
            {
                hasRetry = true;
                maxRetryCount = Math.Max(maxRetryCount, recorded.RetryCount);
            }
        }

        if (hasRetry)
            outcome |= HopDecisionFlags.Retried;

        return (outcome, maxRetryCount > 0 ? maxRetryCount : null);
    }

    private static int? ResolveOutputEmissionCount(IReadOnlyList<int>? contributors, IReadOnlyDictionary<int, int>? outputCountByInput, int inputCount)
    {
        if (contributors is null || contributors.Count == 0 || outputCountByInput is null)
            return null;

        int? resolved = null;

        foreach (var contributorIndex in contributors)
        {
            if (contributorIndex < 0 || contributorIndex >= inputCount)
                continue;

            if (!outputCountByInput.TryGetValue(contributorIndex, out var contributorOutputCount))
                continue;

            if (resolved is null)
            {
                resolved = contributorOutputCount;
                continue;
            }

            if (resolved.Value != contributorOutputCount)
            {
                // A single scalar cannot represent conflicting fan-out counts across contributors.
                return null;
            }
        }

        return resolved;
    }

    protected static IEnumerable<LineagePacket<TOut>> MapMaterialized<TIn, TOut>(List<LineagePacket<TIn>> inputs, List<TOut> outputs, string nodeId,
        Guid pipelineId, string? pipelineName, TransformCardinality card, LineageOptions? opts, Type? mapperType, ILineageMapper? mapperInstance)
    {
        Dictionary<int, IReadOnlyList<int>>? recordsByOutput = null;
        Dictionary<int, int>? outputCountByInput = null;

        if (mapperType is not null && mapperInstance is not null)
        {
            var mappingResult =
                mapperInstance.MapInputToOutputs(inputs.Cast<object>().ToList(), outputs.Cast<object>().ToList(), new LineageMappingContext(nodeId));

            recordsByOutput = mappingResult.Records.ToDictionary(r => r.OutputIndex, r => r.InputIndices);

            outputCountByInput = [];

            foreach (var kvp in recordsByOutput)
            {
                foreach (var inputIndex in kvp.Value)
                {
                    if (inputIndex < 0 || inputIndex >= inputs.Count)
                        continue;

                    outputCountByInput[inputIndex] = outputCountByInput.TryGetValue(inputIndex, out var count)
                        ? count + 1
                        : 1;
                }
            }
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
            IReadOnlyList<int>? contributorsForEmission = null;

            if (recordsByOutput is not null && recordsByOutput.TryGetValue(oi, out var inputIdxs))
            {
                contributorsForEmission = inputIdxs;

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

                if (recordsByOutput is null && inputPacket is not null)
                    contributorsForEmission = [oi];
            }

            var outcome = ancestry is not null && ancestry.Count > 1
                ? HopDecisionFlags.Aggregated
                : HopDecisionFlags.Emitted;

            var (effectiveOutcome, retryCount) = ResolveRecordedOutcome(pipelineId, nodeId, contributorsForEmission, outcome);

            var cardinalityObserved = ancestry is null
                ? ObservedCardinality.Unknown
                : ancestry.Count == 0
                    ? ObservedCardinality.Zero
                    : ancestry.Count == 1
                        ? ObservedCardinality.One
                        : ObservedCardinality.Many;

            int? outputEmissionCount = recordsByOutput is not null
                ? ResolveOutputEmissionCount(contributorsForEmission, outputCountByInput, inputs.Count)
                : inputPacket is not null
                    ? 1
                    : null;

            if (inputPacket is not null)
            {
                var hopRecords = inputPacket.LineageHops;

                if (inputPacket.Collect)
                    hopRecords = AppendHop(hopRecords, nodeId, pipelineId, pipelineName, opts, effectiveOutcome, cardinalityObserved, ancestry,
                        outputEmissionCount, inputPacket.Data, outputData, retryCount);

                yield return new LineagePacket<TOut>(outputData, inputPacket.CorrelationId,
                    inputPacket.TraversalPath.Add(QualifyNodeId(nodeId, pipelineId)))
                { Collect = inputPacket.Collect, LineageHops = hopRecords };
            }
            else
                yield return new LineagePacket<TOut>(outputData, Guid.NewGuid(), ImmutableList.Create(QualifyNodeId(nodeId, pipelineId)))
                {
                    Collect = true,
                    LineageHops = ImmutableList<LineageHop>.Empty,
                };
        }
    }

    protected static async IAsyncEnumerable<LineagePacket<TOut>> PositionalStreamingMap<TIn, TOut>(IAsyncEnumerable<LineagePacket<TIn>> inAll,
        IAsyncEnumerable<TOut> outAll, string nodeId, Guid pipelineId, string? pipelineName, TransformCardinality card, LineageOptions? opts,
        [EnumeratorCancellation] CancellationToken token)
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
                var (effectiveOutcome, retryCount) = ResolveRecordedOutcome(pipelineId, nodeId, matchedInputCount, HopDecisionFlags.Emitted);

                if (inputPacket.Collect)
                    hopRecords = MaybeAppendHop(hopRecords, nodeId, pipelineId, pipelineName, opts, 1, inputPacket.Data, outputData,
                        effectiveOutcome, retryCount);

                yield return new LineagePacket<TOut>(outputData, inputPacket.CorrelationId,
                    inputPacket.TraversalPath.Add(QualifyNodeId(nodeId, pipelineId)))
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
