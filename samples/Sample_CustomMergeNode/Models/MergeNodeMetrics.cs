using System;

namespace Sample_CustomMergeNode.Models;

/// <summary>
///     Metrics for tracking merge node performance
/// </summary>
public class MergeNodeMetrics
{
    public MergeNodeMetrics()
    {
        StartTime = DateTime.UtcNow;
        LastMergeTime = StartTime;
    }

    public long TotalTicksProcessed { get; private set; }
    public long ConflictsResolved { get; private set; }
    public long QualityImprovements { get; private set; }
    public double AverageProcessingTimeMs { get; private set; }
    public int ActiveSources { get; private set; }
    public int BufferSize { get; private set; }
    public long TotalMerges { get; private set; }
    public long SuccessfulMerges { get; private set; }
    public long FailedMerges { get; private set; }
    public DateTime LastMergeTime { get; private set; }
    public DateTime StartTime { get; }

    public void IncrementMergeOperations()
    {
        TotalMerges++;
        LastMergeTime = DateTime.UtcNow;
    }

    public void IncrementSuccessfulMerges()
    {
        SuccessfulMerges++;
    }

    public void IncrementFailedMerges()
    {
        FailedMerges++;
    }

    public void UpdateProcessingTime(TimeSpan processingTime)
    {
        var newTimeMs = processingTime.TotalMilliseconds;
        AverageProcessingTimeMs = (AverageProcessingTimeMs * (TotalMerges - 1) + newTimeMs) / TotalMerges;
    }

    public void IncrementConflictsResolved()
    {
        ConflictsResolved++;
    }

    public void IncrementQualityImprovements()
    {
        QualityImprovements++;
    }

    public void UpdateActiveSources(int count)
    {
        ActiveSources = count;
    }

    public void UpdateBufferSize(int size)
    {
        BufferSize = size;
    }

    public void IncrementTotalTicksProcessed(long count = 1)
    {
        TotalTicksProcessed += count;
    }
}
