namespace Sample_ParallelExecution_Simplified;

/// <summary>
///     Simple data model for demonstration.
/// </summary>
public class TaskData
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int DurationMs { get; set; }
}

/// <summary>
///     Result of processing a task.
/// </summary>
public class ProcessedResult
{
    public int Id { get; set; }
    public string OriginalName { get; set; } = string.Empty;
    public string ProcessedName { get; set; } = string.Empty;
    public long ElapsedMs { get; set; }
}
