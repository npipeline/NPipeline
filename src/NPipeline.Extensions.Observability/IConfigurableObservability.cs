using NPipeline.Observability.Configuration;

namespace NPipeline.Observability;

/// <summary>
///     Interface for nodes that want to provide custom observability configuration.
/// </summary>
/// <remarks>
///     <para>
///         Implement this interface on your node class to provide node-specific
///         observability settings that will be used by the framework when the node executes.
///     </para>
///     <para>
///         This interface is an alternative to configuring observability via the pipeline builder.
///         If both builder configuration and this interface are present, the builder configuration
///         takes precedence.
///     </para>
/// </remarks>
/// <example>
///     <code>
///     public class MemoryIntensiveNode : TransformNode&lt;Data, Result&gt;, IConfigurableObservability
///     {
///         public ObservabilityOptions GetObservabilityOptions() =&gt; new()
///         {
///             RecordTiming = true,
///             RecordItemCounts = true,
///             RecordMemoryUsage = true // Track memory for this intensive operation
///         };
///         
///         public override Task&lt;Result&gt; ExecuteAsync(Data item, PipelineContext context, CancellationToken ct)
///         {
///             // Your implementation - metrics recorded automatically
///             return Task.FromResult(Process(item));
///         }
///     }
///     </code>
/// </example>
public interface IConfigurableObservability
{
    /// <summary>
    ///     Gets the observability options for this node.
    /// </summary>
    /// <returns>The observability options to use when executing this node.</returns>
    ObservabilityOptions GetObservabilityOptions();
}
