using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using NPipeline.Lineage;
using NPipeline.Sampling;

namespace NPipeline.DataFlow
{
    /// <summary>
    /// Wraps a stream and emits sampled lineage-aware records through <see cref="IPipelineSampleRecorder"/>.
    /// </summary>
    public sealed class SamplingDataStream<T>(
        IDataStream<T> inner,
        string nodeId,
        string direction,
        IPipelineSampleRecorder recorder,
        int sampleRate = 100,
        string? pipelineName = null,
        Guid? runId = null) : IForwardOnlyDataStream<T>
    {
        private static readonly JsonSerializerOptions SafeSerializeOptions = new()
        {
            ReferenceHandler = ReferenceHandler.IgnoreCycles,
            MaxDepth = 16,
        };

        private static readonly JsonSerializerOptions SafeDeserializeOptions = new()
        {
            PropertyNameCaseInsensitive = true,
        };

        private readonly int _sampleRate = Math.Max(1, sampleRate);

        /// <summary>
        /// Gets the node identifier associated with sampled records.
        /// </summary>
        public string NodeId => nodeId;

        /// <summary>
        /// Gets the sampling direction label.
        /// </summary>
        public string Direction => direction;

        /// <summary>
        /// Gets the optional pipeline name attached to recorded samples.
        /// </summary>
        public string? PipelineName => pipelineName;

        /// <summary>
        /// Gets the optional run identifier attached to recorded samples.
        /// </summary>
        public Guid? RunId => runId;

        /// <summary>
        /// Gets the wrapped stream name.
        /// </summary>
        public string StreamName => inner.StreamName;

        /// <summary>
        /// Gets the runtime data type emitted by the wrapped stream.
        /// </summary>
        public Type GetDataType()
        {
            return inner.GetDataType();
        }

        /// <summary>
        /// Returns an enumerator over sampled stream items.
        /// </summary>
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return Enumerate(cancellationToken).GetAsyncEnumerator(cancellationToken);
        }

        /// <summary>
        /// Projects sampled items as an object-typed asynchronous sequence.
        /// </summary>
        public async IAsyncEnumerable<object?> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await foreach (var item in Enumerate(cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
        }

        /// <summary>
        /// Disposes the wrapped stream.
        /// </summary>
        public ValueTask DisposeAsync()
        {
            return inner.DisposeAsync();
        }

        private async IAsyncEnumerable<T> Enumerate([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            long itemCount = 0;

            await foreach (var item in inner.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                itemCount++;

                // Adaptive sampling keeps full fidelity for short streams, then bounds cost for long runs.
                if (itemCount <= _sampleRate || itemCount % _sampleRate == 0)
                {
                    if (item is ILineageEnvelope envelope)
                    {
                        int[]? ancestryInputIndices = null;
                        var outcome = SampleOutcome.Success;
                        var retryCount = 0;

                        if (envelope.LineageHops.Count > 0)
                        {
                            var latestHop = envelope.LineageHops[^1];

                            if (latestHop.AncestryInputIndices is { Count: > 0 })
                            {
                                ancestryInputIndices = [.. latestHop.AncestryInputIndices];
                            }

                            outcome = DetermineOutcome(latestHop.Outcome);
                            retryCount = DetermineRetryCount(envelope.LineageHops);
                        }

                        var serialized = SafeSerialize(envelope.Data);

                        recorder.RecordSample(
                            nodeId,
                            direction,
                            envelope.CorrelationId,
                            ancestryInputIndices,
                            serialized,
                            DateTimeOffset.UtcNow,
                            pipelineName,
                            runId,
                            outcome,
                            retryCount);
                    }
                }

                yield return item;
            }
        }

        private static SampleOutcome DetermineOutcome(HopDecisionFlags flags)
        {
            if ((flags & HopDecisionFlags.DeadLettered) != 0)
            {
                return SampleOutcome.DeadLetter;
            }

            if ((flags & HopDecisionFlags.Error) != 0)
            {
                return SampleOutcome.Error;
            }

            return (flags & HopDecisionFlags.FilteredOut) != 0
                ? SampleOutcome.Skipped
                : SampleOutcome.Success;
        }

        private static int DetermineRetryCount(IReadOnlyList<LineageHop> lineageHops)
        {
            if (lineageHops.Count == 0)
            {
                return 0;
            }

            var latestHop = lineageHops[^1];

            if (latestHop.RetryCount is > 0)
            {
                return latestHop.RetryCount.Value;
            }

            if ((latestHop.Outcome & HopDecisionFlags.Retried) == 0)
            {
                return 0;
            }

            var retryCount = 0;

            for (var index = lineageHops.Count - 1; index >= 0; index--)
            {
                var hop = lineageHops[index];

                if (!string.Equals(hop.NodeId, latestHop.NodeId, StringComparison.Ordinal))
                {
                    break;
                }

                if ((hop.Outcome & HopDecisionFlags.Retried) != 0)
                {
                    retryCount++;
                }
            }

            return Math.Max(1, retryCount);
        }

        private static object? SafeSerialize(object? item)
        {
            if (item is null)
            {
                return null;
            }

            try
            {
                var json = JsonSerializer.Serialize(item, SafeSerializeOptions);
                return JsonSerializer.Deserialize<object>(json, SafeDeserializeOptions);
            }
#pragma warning disable CA1031
            catch
            {
                return new
                {
                    Error = "sample_serialization_failed",
                    ItemType = item.GetType().FullName,
                };
            }
#pragma warning restore CA1031
        }
    }
}
