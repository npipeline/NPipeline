using System.Diagnostics.CodeAnalysis;

namespace NPipeline.Connectors.DeliverySemantics;

/// <summary>
///     Delivery guarantees for connector operations.
/// </summary>
[SuppressMessage("Design", "CA1028:EnumStorageShouldBeInt32", Justification = "Explicit values aid serialization stability.")]
public enum DeliverySemantics : byte
{
    /// <summary>
    ///     Messages may be redelivered; retries are allowed.
    /// </summary>
    AtLeastOnce = 0,

    /// <summary>
    ///     Messages are delivered at most once; failures drop data.
    /// </summary>
    AtMostOnce = 1,

    /// <summary>
    ///     Messages are processed exactly once using idempotency or transactions.
    /// </summary>
    ExactlyOnce = 2,
}
