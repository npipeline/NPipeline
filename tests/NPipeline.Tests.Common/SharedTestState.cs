using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

namespace NPipeline.Tests.Common;

[SuppressMessage("Usage", "CA2211:Non-constant fields should not be visible")]

// Static state containers for test instrumentation
public static class SharedTestState
{
    public static int Current;
    public static int Peak;
    public static readonly object Gate = new();
    public static int Count;
    public static int DelayMs;
    public static List<int> Collected = [];
    public static readonly ConcurrentDictionary<int, int> AttemptCounts = new();

    public static void Reset(int count, int delayMs)
    {
        Current = 0;
        Peak = 0;
        Count = count;
        DelayMs = delayMs;
        Collected = [];
        AttemptCounts.Clear();
    }
}
