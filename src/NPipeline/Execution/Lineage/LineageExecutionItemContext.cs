namespace NPipeline.Execution.Lineage;

internal readonly record struct LineageExecutionItemMetadata(Guid CorrelationId, int[]? AncestryInputIndices);

internal static class LineageExecutionItemContext
{
    private static readonly AsyncLocal<long?> CurrentInputIndex = new();
    private static readonly AsyncLocal<LineageExecutionItemMetadata?> CurrentItemMetadata = new();

    public static void SetCurrentInputIndex(long index)
    {
        CurrentInputIndex.Value = index;
    }

    public static void SetCurrentInputContext(long index, Guid correlationId, int[]? ancestryInputIndices = null)
    {
        CurrentInputIndex.Value = index;
        CurrentItemMetadata.Value = new LineageExecutionItemMetadata(correlationId, ancestryInputIndices);
    }

    public static bool TryGetCurrentInputIndex(out long index)
    {
        if (CurrentInputIndex.Value is long value)
        {
            index = value;
            return true;
        }

        index = default;
        return false;
    }

    public static bool TryGetCurrentItemMetadata(out LineageExecutionItemMetadata metadata)
    {
        if (CurrentItemMetadata.Value is LineageExecutionItemMetadata value)
        {
            metadata = value;
            return true;
        }

        metadata = default;
        return false;
    }

    public static void ClearCurrentInputIndex()
    {
        CurrentInputIndex.Value = null;
        CurrentItemMetadata.Value = null;
    }
}
