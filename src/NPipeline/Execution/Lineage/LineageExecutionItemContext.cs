using System.Threading;

namespace NPipeline.Execution.Lineage;

internal static class LineageExecutionItemContext
{
    private static readonly AsyncLocal<long?> CurrentInputIndex = new();

    public static void SetCurrentInputIndex(long index)
    {
        CurrentInputIndex.Value = index;
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

    public static void ClearCurrentInputIndex()
    {
        CurrentInputIndex.Value = null;
    }
}
