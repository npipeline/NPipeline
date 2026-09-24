using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Lineage;

/// <summary>
///     An execution strategy that reports, for each output it yields, which input item produced it (see
///     <see cref="LineageNodeOutcomeWriter.ReportOutput" />), and reports each input item that ends without an output.
/// </summary>
/// <remarks>
///     Implemented only by strategies whose every output path reports, because the lineage mapper trusts the reports
///     instead of pairing outputs with inputs by position. A subclass that replaces how outputs are produced must not
///     inherit the claim, so implementations check their own type.
/// </remarks>
internal interface ILineageProvenanceStrategy
{
    /// <summary>
    ///     Whether this strategy reports provenance when it runs <paramref name="node" />.
    /// </summary>
    bool ReportsLineageProvenance(INode node);
}

/// <summary>
///     A stream transform node that reports the provenance of each output it yields, and each input item it drops.
/// </summary>
internal interface ILineageProvenanceNode;

internal static class LineageProvenanceSupport
{
    /// <summary>
    ///     Whether running <paramref name="node" /> under <paramref name="strategy" /> reports the provenance of each
    ///     output, so its lineage can be mapped by input index instead of by position.
    /// </summary>
    public static bool Reports(IExecutionStrategy strategy, INode node)
    {
        return strategy switch
        {
            ResilientExecutionStrategy resilient => Reports(
                resilient.InnerStrategy ?? (node as IExecutionStrategyProvider)?.DefaultExecutionStrategy ?? SequentialExecutionStrategy.Instance,
                node),
            StreamPassthroughExecutionStrategy => node is ILineageProvenanceNode,
            ILineageProvenanceStrategy reporting => reporting.ReportsLineageProvenance(node),
            _ => false,
        };
    }

    /// <summary>
    ///     The lineage state of the node <paramref name="node" /> runs as, for a stream transform that reports its own
    ///     provenance. Inactive when lineage is off or the node is run outside a pipeline.
    /// </summary>
    public static LineageNodeOutcomeWriter WriterFor(INode node, PipelineContext context) =>
        context.NodeEnvironment.TryGetNodeId(node, out var nodeId)
            ? LineageNodeOutcomeRegistry.GetWriter(context.RunIdentity.PipelineId, nodeId)
            : default;
}
