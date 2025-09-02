using System.Collections.Frozen;
using System.Collections.Immutable;
using NPipeline.Diagnostics.Export;
using NPipeline.Graph;

namespace NPipeline.Pipeline;

/// <summary>
///     Fluent extensions for validating pipeline structures and detecting common issues before building.
/// </summary>
/// <remarks>
///     These extensions allow you to validate and analyze your pipeline at any point during builder configuration,
///     without requiring a full build. This enables early error detection and helps with debugging complex pipelines.
/// </remarks>
public static class PipelineBuilderValidationExtensions
{
    /// <summary>
    ///     Validates the current pipeline structure without building the complete pipeline.
    /// </summary>
    /// <param name="builder">The pipeline builder to validate.</param>
    /// <returns>A validation result containing any structural issues found.</returns>
    /// <remarks>
    ///     This method performs all standard validation checks (cycles, type compatibility, connectivity)
    ///     without freezing the builder state or requiring all configuration to be finalized.
    ///     Use this to get early feedback on pipeline validity during development.
    ///     Example:
    ///     <code>
    /// var builder = new PipelineBuilder()
    ///     .AddSource&lt;MySource, int&gt;("source")
    ///     .AddTransform&lt;MyTransform, int, string&gt;("transform");
    /// 
    /// var result = builder.Validate();
    /// if (!result.IsValid)
    /// {
    ///     foreach (var error in result.Errors)
    ///         Console.WriteLine($"Error: {error}");
    /// }
    /// </code>
    /// </remarks>
    public static PipelineValidationResult Validate(this PipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var graph = BuildGraphFromBuilder(builder);
        return PipelineGraphValidator.Validate(graph);
    }

    /// <summary>
    ///     Checks if a connection between two nodes is valid before adding it to the pipeline.
    /// </summary>
    /// <typeparam name="TData">The data type that must match between source and target.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="source">The source node handle (must produce TData).</param>
    /// <param name="target">The target node handle (must consume TData).</param>
    /// <param name="reason">Output parameter containing the reason if the connection is invalid.</param>
    /// <returns>True if the connection is valid and can be made; false otherwise.</returns>
    /// <remarks>
    ///     This method checks:
    ///     - Type compatibility between source output and target input
    ///     - Whether the connection would create a cycle in the graph
    ///     - Whether both nodes exist in the builder
    ///     Example:
    ///     <code>
    /// var source = builder.AddSource&lt;MySource, int&gt;("source");
    /// var transform = builder.AddTransform&lt;MyTransform, int, string&gt;("transform");
    /// 
    /// if (builder.CanConnect(source, transform, out var reason))
    /// {
    ///     builder.Connect(source, transform);
    /// }
    /// else
    /// {
    ///     Console.WriteLine($"Cannot connect: {reason}");
    /// }
    /// </code>
    /// </remarks>
    public static bool CanConnect<TData>(
        this PipelineBuilder builder,
        IOutputNodeHandle<TData> source,
        IInputNodeHandle<TData> target,
        out string? reason)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(target);

        reason = null;

        // Check if nodes exist
        if (!builder.NodeState.Nodes.ContainsKey(source.Id))
        {
            reason = $"Source node '{source.Id}' not found in pipeline.";
            return false;
        }

        if (!builder.NodeState.Nodes.ContainsKey(target.Id))
        {
            reason = $"Target node '{target.Id}' not found in pipeline.";
            return false;
        }

        // Check for self-loop
        if (source.Id == target.Id)
        {
            reason = "Cannot connect a node to itself (self-loop).";
            return false;
        }

        // Check if connection would create a cycle
        if (WouldCreateCycle(builder, source.Id, target.Id))
        {
            reason = $"Connection from '{source.Id}' to '{target.Id}' would create a cycle in the pipeline graph.";
            return false;
        }

        return true;
    }

    /// <summary>
    ///     Generates a Mermaid flowchart diagram of the current pipeline structure.
    /// </summary>
    /// <param name="builder">The pipeline builder to visualize.</param>
    /// <returns>A string containing Mermaid flowchart syntax (graph TD).</returns>
    /// <remarks>
    ///     This method creates a visual representation of the pipeline graph in Mermaid format,
    ///     which can be rendered by tools like Mermaid Live Editor, GitHub, and many documentation platforms.
    ///     The diagram shows all nodes and their connections. Nodes are labeled with their name and kind,
    ///     and edges may be labeled with port information if specified.
    ///     Example:
    ///     <code>
    /// var builder = new PipelineBuilder()
    ///     .AddSource&lt;MySource, int&gt;("source")
    ///     .AddTransform&lt;MyTransform, int, string&gt;("transform")
    ///     .AddSink&lt;MySink, string&gt;("sink");
    /// 
    /// var mermaid = builder.ToMermaidDiagram();
    /// Console.WriteLine(mermaid);
    /// // Output:
    /// // graph TD
    /// //     source["source : Source"]
    /// //     transform["transform : Transform"]
    /// //     sink["sink : Sink"]
    /// //     source --> transform
    /// //     transform --> sink
    /// </code>
    /// </remarks>
    public static string ToMermaidDiagram(this PipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var graph = BuildGraphFromBuilder(builder);
        return PipelineGraphExporter.ToMermaid(graph);
    }

    /// <summary>
    ///     Gets a human-readable textual description of the current pipeline structure.
    /// </summary>
    /// <param name="builder">The pipeline builder to describe.</param>
    /// <returns>A formatted string describing all nodes and edges in the pipeline.</returns>
    /// <remarks>
    ///     This provides a detailed text-based view of the pipeline including:
    ///     - All nodes with their ID, name, kind, and type information
    ///     - All edges showing connections between nodes
    ///     This is useful for debugging and logging pipeline structures.
    ///     Example:
    ///     <code>
    /// var description = builder.Describe();
    /// Console.WriteLine(description);
    /// // Output shows detailed node and edge information
    /// </code>
    /// </remarks>
    public static string Describe(this PipelineBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var graph = BuildGraphFromBuilder(builder);
        return PipelineGraphExporter.Describe(graph);
    }

    /// <summary>
    ///     Checks if a potential connection would create a cycle in the pipeline graph.
    /// </summary>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="sourceId">The ID of the source node.</param>
    /// <param name="targetId">The ID of the target node.</param>
    /// <returns>True if creating an edge from sourceId to targetId would create a cycle; false otherwise.</returns>
    /// <remarks>
    ///     Uses depth-first search to detect if a path already exists from targetId back to sourceId.
    ///     If such a path exists, adding the edge would create a cycle.
    /// </remarks>
    private static bool WouldCreateCycle(PipelineBuilder builder, string sourceId, string targetId)
    {
        // Build adjacency list of current edges
        Dictionary<string, List<string>> adjacency = new();

        foreach (var node in builder.NodeState.Nodes.Keys)
        {
            adjacency[node] = [];
        }

        foreach (var edge in builder.ConnectionState.Edges)
        {
            adjacency[edge.SourceNodeId].Add(edge.TargetNodeId);
        }

        // DFS to check if there's a path from targetId to sourceId
        HashSet<string> visited = new();
        return DfsDetectsCycle(adjacency, targetId, sourceId, visited);
    }

    private static bool DfsDetectsCycle(
        Dictionary<string, List<string>> adjacency,
        string current,
        string target,
        HashSet<string> visited)
    {
        if (current == target)
            return true; // Found path to target - would create cycle

        if (visited.Contains(current))
            return false; // Already visited this node

        _ = visited.Add(current);

        if (!adjacency.TryGetValue(current, out var neighbors))
            return false;

        foreach (var neighbor in neighbors)
        {
            if (DfsDetectsCycle(adjacency, neighbor, target, visited))
                return true;
        }

        return false;
    }

    /// <summary>
    ///     Builds a PipelineGraph from the current builder state without finalizing the pipeline.
    /// </summary>
    private static PipelineGraph BuildGraphFromBuilder(PipelineBuilder builder)
    {
        var nodesList = builder.NodeState.Nodes.Values.ToImmutableList();
        var nodeDefinitionMap = nodesList.ToFrozenDictionary(n => n.Id);

        var graph = PipelineGraphBuilder.Create()
            .WithNodes(nodesList)
            .WithEdges(builder.ConnectionState.Edges.ToImmutableList())
            .WithPreconfiguredNodeInstances(builder.NodeState.PreconfiguredNodeInstances.ToImmutableDictionary())
            .WithNodeDefinitionMap(nodeDefinitionMap)
            .Build();

        return graph;
    }
}
