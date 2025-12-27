namespace NPipeline.Nodes.Internal;

/// <summary>
///     Internal wrapper type that tags items from the left stream in a self-join scenario.
/// </summary>
/// <typeparam name="T">The type of the wrapped item.</typeparam>
internal sealed record LeftWrapper<T>(T Item) : ISelfJoinWrapper
{
    object? ISelfJoinWrapper.Item => Item;
}
