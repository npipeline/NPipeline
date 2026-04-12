using System.Text.Json;
using System.Text.Json.Serialization;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Lineage;
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

    public static void TryRecordError<T>(
        PipelineContext context,
        string nodeId,
        T item,
        Exception exception,
        int retryCount,
        Guid? correlationIdOverride = null,
        int[]? ancestryInputIndicesOverride = null,
        string? originNodeId = null)
    {
        if (!TryGetRecorder(context, out var recorder))
            return;

        Guid correlationId;
        int[]? ancestryInputIndices;

        if (correlationIdOverride is Guid overrideCorrelationId)
        {
            correlationId = overrideCorrelationId;
            ancestryInputIndices = ancestryInputIndicesOverride;
        }
        else
        {
            if (!LineageExecutionItemContext.TryGetCurrentItemMetadata(out var currentMetadata))
                return;

            correlationId = currentMetadata.CorrelationId;
            ancestryInputIndices = currentMetadata.AncestryInputIndices;
        }

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
            context.PipelineName,
            context.RunId == Guid.Empty
                ? null
                : context.RunId,
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
