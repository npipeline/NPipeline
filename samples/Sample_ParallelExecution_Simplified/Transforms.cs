using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_ParallelExecution_Simplified;

/// <summary>
///     Transform nodes demonstrating three configuration approaches for parallelism.
/// </summary>

// Manual Configuration API - Fully explicit control
public class ManualConfigTransform : TransformNode<TaskData, ProcessedResult>
{
    public override async Task<ProcessedResult> ExecuteAsync(TaskData input, PipelineContext context, CancellationToken cancellationToken)
    {
        var start = DateTime.UtcNow;

        // Simulate I/O work
        await Task.Delay(input.DurationMs, cancellationToken);

        var elapsed = DateTime.UtcNow - start;

        return new ProcessedResult
        {
            Id = input.Id,
            OriginalName = input.Name,
            ProcessedName = input.Name.ToUpperInvariant(),
            ElapsedMs = elapsed.Milliseconds,
        };
    }
}

// Preset API - Simplified configuration with workload types
public class PresetConfigTransform : TransformNode<TaskData, ProcessedResult>
{
    public override async Task<ProcessedResult> ExecuteAsync(TaskData input, PipelineContext context, CancellationToken cancellationToken)
    {
        var start = DateTime.UtcNow;

        // Simulate I/O work
        await Task.Delay(input.DurationMs, cancellationToken);

        var elapsed = DateTime.UtcNow - start;

        return new ProcessedResult
        {
            Id = input.Id,
            OriginalName = input.Name,
            ProcessedName = input.Name.ToUpperInvariant(),
            ElapsedMs = elapsed.Milliseconds,
        };
    }
}

// Builder API - Fine-grained control with fluent configuration
public class BuilderConfigTransform : TransformNode<TaskData, ProcessedResult>
{
    public override async Task<ProcessedResult> ExecuteAsync(TaskData input, PipelineContext context, CancellationToken cancellationToken)
    {
        var start = DateTime.UtcNow;

        // Simulate I/O work
        await Task.Delay(input.DurationMs, cancellationToken);

        var elapsed = DateTime.UtcNow - start;

        return new ProcessedResult
        {
            Id = input.Id,
            OriginalName = input.Name,
            ProcessedName = input.Name.ToUpperInvariant(),
            ElapsedMs = elapsed.Milliseconds,
        };
    }
}
