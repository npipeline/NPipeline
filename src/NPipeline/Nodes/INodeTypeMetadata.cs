namespace NPipeline.Nodes;

/// <summary>
///     Provides statically-known input/output type metadata for a node to avoid reflection at runtime.
/// </summary>
public interface INodeTypeMetadata
{
    Type? InputType { get; }
    Type? OutputType { get; }
}
