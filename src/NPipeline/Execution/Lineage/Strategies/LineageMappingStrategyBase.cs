using System.Collections.Immutable;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.Lineage;

namespace NPipeline.Execution.Lineage.Strategies;

/// <summary>
///     Base class providing shared lineage mapping helper methods for strategy implementations.
/// </summary>
internal abstract class LineageMappingStrategyBase
{
    private static readonly JsonSerializerOptions SnapshotSerializerOptions = new(JsonSerializerDefaults.Web)
    {
        ReferenceHandler = ReferenceHandler.IgnoreCycles,
    };

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static object? SnapshotValue(object? value, LineageOptions? opts)
    {
        if (opts?.CaptureHopSnapshots != true || value is null)
            return null;

        try
        {
            return JsonSerializer.SerializeToElement(value, SnapshotSerializerOptions);
        }
        catch
        {
            return value.ToString();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static string QualifyNodeId(string nodeId, Guid pipelineId)
    {
        return $"{pipelineId:N}::{nodeId}";
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int OutcomePriority(LineageOutcomeReason reason)
    {
        return reason switch
        {
            LineageOutcomeReason.DeadLettered => 700,
            LineageOutcomeReason.Error => 600,
            LineageOutcomeReason.FilteredOut => 500,
            LineageOutcomeReason.DroppedByBackpressure => 400,
            LineageOutcomeReason.Aggregated => 300,
            LineageOutcomeReason.Joined => 200,
            LineageOutcomeReason.ConsumedWithoutEmission => 150,
            _ => 100,
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static LineageOutcomeReason MergeOutcomeReason(LineageOutcomeReason current, LineageOutcomeReason candidate)
    {
        return OutcomePriority(candidate) >= OutcomePriority(current)
            ? candidate
            : current;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ImmutableList<LineageRecord> AppendRecord(
        ImmutableList<LineageRecord> existing,
        Guid correlationId,
        IReadOnlyList<string> traversalPath,
        string nodeId,
        Guid pipelineId,
        string? pipelineName,
        LineageOptions? opts,
        LineageOutcomeReason outcomeReason,
        bool isTerminal,
        ObservedCardinality cardinality,
        IReadOnlyList<int>? contributorInputIndices,
        IReadOnlyList<Guid>? contributorCorrelationIds,
        int? inputContributorCount,
        int? outputEmissionCount,
        object? inputSnapshot,
        object? outputSnapshot,
        int? retryCount)
    {
        var cap = opts is not null && opts.MaxHopRecordsPerItem > 0
            ? opts.MaxHopRecordsPerItem
            : int.MaxValue;

        if (existing.Count >= cap)
            return existing;

        var truncated = existing.Count + 1 >= cap;

        var normalizedContributors = opts?.IncludeContributorCorrelationIds == true
            ? contributorCorrelationIds
            : null;

        var record = new LineageRecord(
            correlationId,
            nodeId,
            pipelineId,
            outcomeReason,
            isTerminal,
            traversalPath,
            pipelineName,
            DateTimeOffset.UtcNow,
            retryCount,
            normalizedContributors,
            contributorInputIndices,
            inputContributorCount,
            outputEmissionCount,
            cardinality,
            SnapshotValue(inputSnapshot, opts),
            SnapshotValue(outputSnapshot, opts),
            opts?.RedactData == true
                ? null
                : outputSnapshot ?? inputSnapshot)
        {
            Truncated = truncated,
        }.Normalize();

        return existing.Add(record);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static ImmutableList<LineageRecord> MaybeAppendHop(
        ImmutableList<LineageRecord> existing,
        Guid correlationId,
        IReadOnlyList<string> traversalPath,
        string nodeId,
        Guid pipelineId,
        string? pipelineName,
        LineageOptions? opts,
        int? outputEmissionCount,
        object? inputSnapshot,
        object? outputSnapshot,
        LineageOutcomeReason outcomeReason = LineageOutcomeReason.Emitted,
        int? retryCount = null)
    {
        return AppendRecord(
            existing,
            correlationId,
            traversalPath,
            nodeId,
            pipelineId,
            pipelineName,
            opts,
            outcomeReason,
            false,
            ObservedCardinality.One,
            null,
            null,
            null,
            outputEmissionCount,
            inputSnapshot,
            outputSnapshot,
            retryCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static ImmutableList<LineageRecord> AppendHop(
        ImmutableList<LineageRecord> existing,
        Guid correlationId,
        IReadOnlyList<string> traversalPath,
        string nodeId,
        Guid pipelineId,
        string? pipelineName,
        LineageOptions? opts,
        LineageOutcomeReason outcomeReason,
        ObservedCardinality cardinality,
        IReadOnlyList<int>? ancestry,
        IReadOnlyList<Guid>? contributorCorrelationIds,
        int? outputEmissionCount,
        object? inputSnapshot,
        object? outputSnapshot,
        int? retryCount = null)
    {
        return AppendRecord(
            existing,
            correlationId,
            traversalPath,
            nodeId,
            pipelineId,
            pipelineName,
            opts,
            outcomeReason,
            false,
            cardinality,
            ancestry,
            contributorCorrelationIds,
            ancestry?.Count,
            outputEmissionCount,
            inputSnapshot,
            outputSnapshot,
            retryCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static (LineageOutcomeReason Outcome, int? RetryCount) ResolveRecordedOutcome(
        Guid pipelineId,
        string nodeId,
        int inputIndex,
        LineageOutcomeReason baseOutcome)
    {
        if (!LineageNodeOutcomeRegistry.TryGet(pipelineId, nodeId, inputIndex, out var recorded))
            return (baseOutcome, null);

        var retryCount = recorded.RetryCount > 0
            ? recorded.RetryCount
            : (int?)null;

        var outcome = MergeOutcomeReason(baseOutcome, recorded.OutcomeReason);

        return (outcome, retryCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static (LineageOutcomeReason Outcome, int? RetryCount) ResolveRecordedOutcome(
        Guid pipelineId,
        string nodeId,
        IReadOnlyList<int>? contributorIndices,
        LineageOutcomeReason baseOutcome)
    {
        if (contributorIndices is null || contributorIndices.Count == 0)
            return (baseOutcome, null);

        var outcome = baseOutcome;
        var maxRetryCount = 0;

        foreach (var contributorIndex in contributorIndices)
        {
            if (!LineageNodeOutcomeRegistry.TryGet(pipelineId, nodeId, contributorIndex, out var recorded))
                continue;

            outcome = MergeOutcomeReason(outcome, recorded.OutcomeReason);
            maxRetryCount = Math.Max(maxRetryCount, recorded.RetryCount);
        }

        return (outcome, maxRetryCount > 0
            ? maxRetryCount
            : null);
    }

    private static int? ResolveOutputEmissionCount(
        IReadOnlyList<int>? contributors,
        IReadOnlyDictionary<int, int>? outputCountByInput,
        int inputCount)
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
                return null;
        }

        return resolved;
    }

    private static IReadOnlyList<Guid>? ResolveContributorCorrelationIds<TIn>(
        IReadOnlyList<int>? contributorIndices,
        IReadOnlyList<LineagePacket<TIn>> inputs,
        LineageOptions? opts)
    {
        if (opts?.IncludeContributorCorrelationIds != true || contributorIndices is null || contributorIndices.Count == 0)
            return null;

        var ids = new List<Guid>(contributorIndices.Count);

        foreach (var contributorIndex in contributorIndices)
        {
            if (contributorIndex < 0 || contributorIndex >= inputs.Count)
                continue;

            ids.Add(inputs[contributorIndex].CorrelationId);
        }

        return ids.Count > 0
            ? ids.Distinct().OrderBy(static id => id).ToArray()
            : null;
    }

    protected static IEnumerable<LineagePacket<TOut>> MapMaterialized<TIn, TOut>(
        List<LineagePacket<TIn>> inputs,
        List<TOut> outputs,
        string nodeId,
        Guid pipelineId,
        string? pipelineName,
        TransformCardinality card,
        LineageOptions? opts,
        Type? mapperType,
        ILineageMapper? mapperInstance)
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

            var outcomeReason = ancestry is not null && ancestry.Count > 1
                ? LineageOutcomeReason.Aggregated
                : LineageOutcomeReason.Emitted;

            var (effectiveOutcome, retryCount) = ResolveRecordedOutcome(pipelineId, nodeId, contributorsForEmission, outcomeReason);

            var cardinalityObserved = ancestry is null
                ? ObservedCardinality.Unknown
                : ancestry.Count == 0
                    ? ObservedCardinality.Zero
                    : ancestry.Count == 1
                        ? ObservedCardinality.One
                        : ObservedCardinality.Many;

            var outputEmissionCount = recordsByOutput is not null
                ? ResolveOutputEmissionCount(contributorsForEmission, outputCountByInput, inputs.Count)
                : inputPacket is not null
                    ? 1
                    : null;

            var contributorCorrelationIds = ResolveContributorCorrelationIds(contributorsForEmission, inputs, opts);

            if (inputPacket is not null)
            {
                var traversalPath = inputPacket.TraversalPath.Add(QualifyNodeId(nodeId, pipelineId));
                var lineageRecords = inputPacket.LineageRecords;

                if (inputPacket.Collect)
                {
                    lineageRecords = AppendHop(
                        lineageRecords,
                        inputPacket.CorrelationId,
                        traversalPath,
                        nodeId,
                        pipelineId,
                        pipelineName,
                        opts,
                        effectiveOutcome,
                        cardinalityObserved,
                        ancestry,
                        contributorCorrelationIds,
                        outputEmissionCount,
                        inputPacket.Data,
                        outputData,
                        retryCount);
                }

                yield return new LineagePacket<TOut>(outputData, inputPacket.CorrelationId, traversalPath)
                { Collect = inputPacket.Collect, LineageRecords = lineageRecords };
            }
            else
            {
                var correlationId = Guid.NewGuid();
                var traversalPath = ImmutableList.Create(QualifyNodeId(nodeId, pipelineId));
                var records = ImmutableList<LineageRecord>.Empty;

                if (opts?.EmitIntermediateNodeRecords != false)
                {
                    records = AppendHop(
                        records,
                        correlationId,
                        traversalPath,
                        nodeId,
                        pipelineId,
                        pipelineName,
                        opts,
                        LineageOutcomeReason.Emitted,
                        ObservedCardinality.Zero,
                        null,
                        null,
                        null,
                        null,
                        outputData,
                        null);
                }

                yield return new LineagePacket<TOut>(outputData, correlationId, traversalPath)
                {
                    Collect = true,
                    LineageRecords = records,
                };
            }
        }
    }

    protected static async IAsyncEnumerable<LineagePacket<TOut>> PositionalStreamingMap<TIn, TOut>(
        IAsyncEnumerable<LineagePacket<TIn>> inAll,
        IAsyncEnumerable<TOut> outAll,
        string nodeId,
        Guid pipelineId,
        string? pipelineName,
        TransformCardinality card,
        LineageOptions? opts,
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
                var traversalPath = inputPacket.TraversalPath.Add(QualifyNodeId(nodeId, pipelineId));
                var lineageRecords = inputPacket.LineageRecords;
                var (effectiveOutcome, retryCount) = ResolveRecordedOutcome(pipelineId, nodeId, matchedInputCount, LineageOutcomeReason.Emitted);

                if (inputPacket.Collect)
                {
                    lineageRecords = MaybeAppendHop(
                        lineageRecords,
                        inputPacket.CorrelationId,
                        traversalPath,
                        nodeId,
                        pipelineId,
                        pipelineName,
                        opts,
                        1,
                        inputPacket.Data,
                        outputData,
                        effectiveOutcome,
                        retryCount);
                }

                yield return new LineagePacket<TOut>(outputData, inputPacket.CorrelationId, traversalPath)
                { Collect = inputPacket.Collect, LineageRecords = lineageRecords };

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
