using System.Text;
using NPipeline.Graph;

namespace NPipeline.Diagnostics.Export;

/// <summary>
///     Provides export functionality for pipeline graphs to various formats for visualization and debugging.
/// </summary>
public static class PipelineGraphExporter
{
    /// <summary>
    ///     Export the pipeline graph to Mermaid flowchart syntax (TD).
    ///     Example usage:
    ///     var mermaid = PipelineGraphExporter.ToMermaid(pipeline.Graph);
    /// </summary>
    public static string ToMermaid(PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(graph);

        var sb = new StringBuilder();
        sb.AppendLine("graph TD");

        // Mermaid ids are generated rather than derived from node ids: node ids may contain characters Mermaid reads
        // as syntax, may collide after sanitization (for example "a-b" and "a_b"), and may be a reserved word such as
        // "end".
        var ids = new Dictionary<string, string>(StringComparer.Ordinal);
        string MermaidId(string nodeId) => ids.TryGetValue(nodeId, out var mapped) ? mapped : ids[nodeId] = $"n{ids.Count}";

        // Nodes: id[Name : Kind]
        foreach (var n in graph.Nodes)
        {
            var label = $"{n.Name} : {n.Kind}";
            var nodeDecl = MermaidId(n.Id);
            sb.AppendLine($"    {nodeDecl}[\"{EscapeText(label)}\"]");
        }

        // Edges: source --> target
        foreach (var e in graph.Edges)
        {
            var src = MermaidId(e.SourceNodeId);
            var dst = MermaidId(e.TargetNodeId);
            var edgeLabel = BuildEdgeLabel(e);

            if (edgeLabel is null)
                sb.AppendLine($"    {src} --> {dst}");
            else
                sb.AppendLine($"    {src} -- \"{EscapeText(edgeLabel)}\" --> {dst}");
        }

        return sb.ToString();
    }

    /// <summary>
    ///     Export a human-readable description of the pipeline graph (nodes and edges).
    /// </summary>
    public static string Describe(PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(graph);
        var sb = new StringBuilder();

        sb.AppendLine("Nodes:");

        foreach (var n in graph.Nodes)
        {
            var inType = n.InputType?.Name ?? "-";
            var outType = n.OutputType?.Name ?? "-";
            sb.AppendLine($"  {n.Id} | {n.Name} | {n.Kind} | {n.NodeType.Name} | In={inType}, Out={outType}");
        }

        sb.AppendLine();
        sb.AppendLine("Edges:");

        foreach (var e in graph.Edges)
        {
            var edgeLabel = BuildEdgeLabel(e);

            if (edgeLabel is null)
                sb.AppendLine($"  {e.SourceNodeId} --> {e.TargetNodeId}");
            else
                sb.AppendLine($"  {e.SourceNodeId} -[{edgeLabel}]-> {e.TargetNodeId}");
        }

        return sb.ToString();
    }

    private static string? BuildEdgeLabel(Edge e)
    {
        if (e.SourceOutputName is null && e.TargetInputName is null)
            return null;

        if (e.SourceOutputName is not null && e.TargetInputName is not null)
            return $"{e.SourceOutputName} → {e.TargetInputName}";

        if (e.SourceOutputName is not null)
            return e.SourceOutputName;

        return e.TargetInputName;
    }

    // Mermaid reads "#name;" and "#123;" inside a label as an entity, so '#' is escaped first, then the quote. A line
    // break would end the declaration, so it becomes a space.
    private static string EscapeText(string text) => text
        .Replace("#", "#35;", StringComparison.Ordinal)
        .Replace("\"", "#quot;", StringComparison.Ordinal)
        .Replace("\r\n", " ", StringComparison.Ordinal)
        .Replace('\r', ' ')
        .Replace('\n', ' ');
}
