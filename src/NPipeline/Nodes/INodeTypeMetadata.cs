namespace NPipeline.Nodes;

/// <summary>
///     Provides statically-known input/output type metadata for a node to avoid reflection at runtime.
/// </summary>
public interface INodeTypeMetadata
{
    /// <summary>
    ///     Gets the input type of the node. Returns null for source nodes which have no input.
    /// </summary>
    Type? InputType { get; }

    /// <summary>
    ///     Gets the output type of the node. Returns null for sink nodes which produce no output.
    /// </summary>
    Type? OutputType { get; }
}
