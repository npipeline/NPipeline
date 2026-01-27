namespace NPipeline.Connectors.Configuration;

/// <summary>
/// Delivery semantics for database operations.
/// </summary>
public enum DeliverySemantic
{
    /// <summary>
    /// At least once delivery - items may be delivered multiple times but never lost.
    /// </summary>
    AtLeastOnce,

    /// <summary>
    /// At most once delivery - items may be lost but never delivered multiple times.
    /// </summary>
    AtMostOnce,

    /// <summary>
    /// Exactly once delivery - items are delivered exactly once.
    /// </summary>
    ExactlyOnce
}
