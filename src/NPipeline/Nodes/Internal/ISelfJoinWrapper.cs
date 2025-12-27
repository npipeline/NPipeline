namespace NPipeline.Nodes.Internal;

/// <summary>
///     Provides a uniform abstraction for self-join wrapper records so callers can unwrap
///     the original item without relying on dynamic dispatch.
/// </summary>
internal interface ISelfJoinWrapper
{
    /// <summary>
    ///     Gets the wrapped item.
    /// </summary>
    object? Item { get; }
}
