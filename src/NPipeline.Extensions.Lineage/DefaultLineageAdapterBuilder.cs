using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution.Lineage;
using NPipeline.Graph.PipelineDelegates;

namespace NPipeline.Lineage;

internal sealed class DefaultLineageAdapterBuilder
{
    public LineageAdapterDelegate? BuildLineageAdapter<TIn, TOut>(Type? lineageMapperType)
    {
        // Cache strategy and mapper per adapter instance to avoid repeated reflection.
        ILineageMappingStrategy<TIn, TOut>? cachedStrategy = null;
        ILineageMapper? cachedMapper = null;

        if (lineageMapperType is not null)
            cachedMapper = (ILineageMapper)Activator.CreateInstance(lineageMapperType)!;

        return (transformInput, nodeId, pipelineId, pipelineName, declaredCardinality, options, cancellationToken) =>
        {
            var typedInput = (IDataStream<LineagePacket<TIn>>)transformInput;

            // The node executor starts the node's lineage state, knowing whether its strategy reports provenance.
            var nodeLineage = LineageNodeOutcomeRegistry.GetOrBeginNode(pipelineId, nodeId);

            // Read typed input once and fan out packets + raw values via channels.
            var dataChannel = Channel.CreateUnbounded<TIn>(new UnboundedChannelOptions { SingleWriter = true });

            var packetChannel = Channel.CreateUnbounded<LineagePacket<TIn>>(new UnboundedChannelOptions { SingleWriter = true });

            _ = PumpInputAsync(typedInput, dataChannel.Writer, packetChannel.Writer, nodeLineage, cancellationToken);

            var unwrappedPipe = new DataStream<TIn>(dataChannel.Reader.ReadAllAsync(cancellationToken), $"Unwrapped_{typedInput.StreamName}");

            return (unwrappedPipe, RewrapFunc);

            IDataStream RewrapFunc(IDataStream outputPipe)
            {
                var typedOutputPipe = (IDataStream<TOut>)outputPipe;

                var rewrappedStream = RewrapStrategy(
                    packetChannel.Reader.ReadAllAsync(cancellationToken),
                    typedOutputPipe,
                    nodeId,
                    pipelineId,
                    pipelineName,
                    declaredCardinality,
                    options,
                    cancellationToken);

                var cleanupStream = CleanupOnComplete(rewrappedStream, pipelineId, nodeId, cancellationToken);
                return new DataStream<LineagePacket<TOut>>(cleanupStream, $"Rewrapped_{outputPipe.StreamName}");
            }

            IAsyncEnumerable<LineagePacket<TOut>> RewrapStrategy(
                IAsyncEnumerable<LineagePacket<TIn>> inputStream,
                IAsyncEnumerable<TOut> outputStream,
                string currentId,
                Guid currentPipelineId,
                string? currentPipelineName,
                TransformCardinality transformCardinality,
                LineageOptions? lineageOptions,
                CancellationToken ct)
            {
                // A strategy that reports each output's input is mapped by index; a declared mapper still wins, and
                // any other node is mapped by position.
                var strategy = nodeLineage.ReportsProvenance && lineageMapperType is null
                    ? ProvenanceMappingStrategy<TIn, TOut>.Instance
                    : cachedStrategy ??= SelectLineageMappingStrategy<TIn, TOut>(lineageMapperType, transformCardinality, lineageOptions);

                return strategy.MapAsync(
                    inputStream,
                    outputStream,
                    currentId,
                    currentPipelineId,
                    currentPipelineName,
                    transformCardinality,
                    lineageOptions,
                    lineageMapperType,
                    cachedMapper,
                    ct);
            }

            static async IAsyncEnumerable<LineagePacket<TOut>> CleanupOnComplete(
                IAsyncEnumerable<LineagePacket<TOut>> source,
                Guid currentPipelineId,
                string currentNodeId,
                [EnumeratorCancellation] CancellationToken ct = default)
            {
                try
                {
                    await foreach (var packet in source.WithCancellation(ct).ConfigureAwait(false))
                    {
                        yield return packet;
                    }
                }
                finally
                {
                    LineageNodeOutcomeRegistry.ClearNode(currentPipelineId, currentNodeId);
                }
            }
        };

        static async Task PumpInputAsync(
            IDataStream<LineagePacket<TIn>> source,
            ChannelWriter<TIn> dataWriter,
            ChannelWriter<LineagePacket<TIn>> packetWriter,
            LineageNodeOutcomeWriter nodeLineage,
            CancellationToken ct)
        {
            try
            {
                long inputIndex = 0;

                await foreach (var packet in source.WithCancellation(ct).ConfigureAwait(false))
                {
                    int[]? ancestryInputIndices = null;

                    if (packet.LineageRecords.Length > 0)
                    {
                        var latestRecord = packet.LineageRecords[^1];

                        if (latestRecord.ContributorInputIndices is { Count: > 0 })
                            ancestryInputIndices = [.. latestRecord.ContributorInputIndices];
                    }

                    // The strategy looks the item's lineage up by its input index, which a node restart preserves.
                    // Registered before the item is written, so it is there when the strategy reads the item.
                    nodeLineage.RegisterInput(inputIndex, packet.CorrelationId, ancestryInputIndices);

                    // Write packet first so strategy input is available before transform output is consumed.
                    await packetWriter.WriteAsync(packet, ct).ConfigureAwait(false);
                    await dataWriter.WriteAsync(packet.Data, ct).ConfigureAwait(false);
                    inputIndex++;
                }

                packetWriter.TryComplete();
                dataWriter.TryComplete();
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                packetWriter.TryComplete();
                dataWriter.TryComplete();
            }
            catch (Exception ex)
            {
                packetWriter.TryComplete(ex);
                dataWriter.TryComplete(ex);
            }
        }
    }

    public SinkLineageUnwrapDelegate? BuildSinkLineageUnwrapDelegate<TIn>()
    {
        return (lineageInput, lineageSink, sinkNodeId, pipelineId, pipelineName, options, ct) =>
        {
            if (lineageInput is not IDataStream<LineagePacket<TIn>> stronglyTyped)
            {
                var expectedType = typeof(IDataStream<LineagePacket<TIn>>);
                var actualType = lineageInput.GetType();

                throw new InvalidCastException(
                    $"Sink lineage input contract mismatch for expected payload '{typeof(TIn).Name}'. " +
                    $"Expected '{expectedType.AssemblyQualifiedName ?? expectedType.FullName ?? expectedType.Name}' " +
                    $"but got '{actualType.AssemblyQualifiedName ?? actualType.FullName ?? actualType.Name}'.");
            }

            var stream = Project(stronglyTyped, ct);

            return new DataStream<TIn>(stream, $"Unwrapped_{lineageInput.StreamName}");

            async IAsyncEnumerable<TIn> Project(IDataStream<LineagePacket<TIn>> input, [EnumeratorCancellation] CancellationToken token)
            {
                var terminalCorrelations = new HashSet<Guid>();

                // Packets of the same item share the hop records made before they diverged: a node that emits several
                // outputs for one input copies its input's records onto each. Each record is emitted once, found by
                // reference. The table holds its keys weakly, so it keeps no record alive once no packet refers to it.
                var emittedRecords = new ConditionalWeakTable<LineageRecord, object?>();

                await foreach (var packet in input.WithCancellation(token).ConfigureAwait(false))
                {
                    if (lineageSink is not null && packet.Collect)
                    {
                        if (options?.EmitIntermediateNodeRecords != false)
                        {
                            foreach (var record in packet.LineageRecords)
                            {
                                if (emittedRecords.TryAdd(record, null))
                                    await lineageSink.RecordAsync(record, token).ConfigureAwait(false);
                            }
                        }

                        if (options?.EnsurePerInputTerminalRecord != false &&
                            !packet.LineageRecords.Any(static r => r.IsTerminal) &&
                            terminalCorrelations.Add(packet.CorrelationId))
                        {
                            var finalPath = packet.TraversalPath.Add($"{pipelineId:N}::{sinkNodeId}");

                            var latestRecord = packet.LineageRecords.Length > 0
                                ? packet.LineageRecords[^1]
                                : null;

                            var terminalRecord = new LineageRecord(
                                packet.CorrelationId,
                                sinkNodeId,
                                pipelineId,
                                LineageOutcomeReason.ConsumedWithoutEmission,
                                true,
                                finalPath,
                                pipelineName,
                                DateTimeOffset.UtcNow,
                                latestRecord?.RetryCount,
                                options?.IncludeContributorCorrelationIds == true
                                    ? latestRecord?.ContributorCorrelationIds ?? [packet.CorrelationId]
                                    : null,
                                latestRecord?.ContributorInputIndices,
                                latestRecord?.InputContributorCount ?? 1,
                                null,
                                latestRecord?.Cardinality ?? ObservedCardinality.One,
                                null,
                                null,
                                options?.RedactData == true
                                    ? null
                                    : packet.Data).Normalize();

                            await lineageSink.RecordAsync(terminalRecord, token).ConfigureAwait(false);
                        }
                    }

                    yield return packet.Data;
                }
            }
        };
    }

    private static ILineageMappingStrategy<TIn, TOut> SelectLineageMappingStrategy<TIn, TOut>(
        Type? mapperType,
        TransformCardinality cardinality,
        LineageOptions? options)
    {
        if (mapperType is null && cardinality == TransformCardinality.OneToOne)
            return StreamingOneToOneStrategy<TIn, TOut>.Instance;

        var cap = options?.MaterializationCap;

        if (cap is not null && cap > 0)
            return CapAwareMaterializingStrategy<TIn, TOut>.Instance;

        return MaterializingStrategy<TIn, TOut>.Instance;
    }
}
