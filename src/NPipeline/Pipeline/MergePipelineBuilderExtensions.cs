using NPipeline.Execution.Annotations;

namespace NPipeline.Pipeline;

/// <summary>
///     Extension methods for bounding fan-in: how many items a node with several inputs (a merge or a join) buffers from
///     its inputs before they wait for it to catch up.
/// </summary>
public static class MergePipelineBuilderExtensions
{
    /// <summary>
    ///     Bounds the fan-in buffer of one node with several inputs. Overrides <see cref="WithGlobalMergeCapacity" />.
    /// </summary>
    /// <param name="builder">The pipeline builder to configure.</param>
    /// <param name="nodeId">The id of the node whose inputs are merged.</param>
    /// <param name="capacity">The most items buffered across the node's inputs. Must be positive.</param>
    /// <returns>The configured pipeline builder for method chaining.</returns>
    public static PipelineBuilder WithMergeCapacity(this PipelineBuilder builder, string nodeId, int capacity)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);

        builder.SetNodeExecutionOption(ExecutionAnnotationKeys.MergeCapacityForNode(nodeId), capacity);
        return builder;
    }

    /// <summary>
    ///     Bounds the fan-in buffer of every node with several inputs, unless a node sets its own with
    ///     <see cref="WithMergeCapacity" />. The default is 1,024 items.
    /// </summary>
    /// <param name="builder">The pipeline builder to configure.</param>
    /// <param name="capacity">The most items buffered across a node's inputs. Must be positive.</param>
    /// <returns>The configured pipeline builder for method chaining.</returns>
    public static PipelineBuilder WithGlobalMergeCapacity(this PipelineBuilder builder, int capacity)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);

        return builder.SetGlobalAnnotation(ExecutionAnnotationKeys.GlobalMergeCapacityKey, capacity);
    }
}
