using System.Text.Json;
using System.Text.Json.Serialization;
using NPipeline.ErrorHandling;
using NPipeline.Lineage;
using NPipeline.Pipeline;

namespace NPipeline.Sampling;

internal static class PipelineSampleErrorReporter
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

    /// <summary>
    ///     Records an item's failure with the recorder in the context, if there is one.
    /// </summary>
    /// <param name="context">The pipeline context holding the recorder.</param>
    /// <param name="nodeId">The node that made the error handling decision.</param>
    /// <param name="item">The failed item.</param>
    /// <param name="exception">The failure.</param>
    /// <param name="retryCount">The retries made before the failure was final.</param>
    /// <param name="correlationId">The item's lineage correlation id.</param>
    /// <param name="ancestryInputIndices">The contributor indices of the hop that produced the item.</param>
    /// <param name="originNodeId">The node where the failure originated, when known; otherwise it is resolved from the exception.</param>
    public static void TryRecordError<T>(
        PipelineContext context,
        string nodeId,
        T item,
        Exception exception,
        int retryCount,
        Guid correlationId,
        int[]? ancestryInputIndices,
        string? originNodeId = null)
    {
        if (!TryGetRecorder(context, out var recorder))
            return;

        var serialized = item is ILineageEnvelope envelope
            ? SafeSerialize(envelope.Data)
            : SafeSerialize(item);

        var effectiveOriginNodeId = originNodeId
                                    ?? FailureAttributionResolver.Resolve(exception, context, nodeId, retryCount).OriginNodeId;

        recorder.RecordError(
            nodeId,
            effectiveOriginNodeId,
            correlationId,
            ancestryInputIndices,
            serialized,
            exception.Message,
            exception.GetType().FullName,
            exception.StackTrace,
            Math.Max(0, retryCount),
            context.RunIdentity.PipelineName,
            context.RunIdentity.RunId == Guid.Empty
                ? null
                : context.RunIdentity.RunId,
            DateTimeOffset.UtcNow);
    }

    private static bool TryGetRecorder(PipelineContext context, out IPipelineSampleRecorder recorder)
    {
        if (context.Properties.TryGetValue(PipelineContextKeys.SampleRecorder, out var value) &&
            value is IPipelineSampleRecorder typedRecorder)
        {
            recorder = typedRecorder;
            return true;
        }

        recorder = NullPipelineSampleRecorder.Instance;
        return false;
    }

    private static object? SafeSerialize(object? item)
    {
        if (item is null)
            return null;

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
