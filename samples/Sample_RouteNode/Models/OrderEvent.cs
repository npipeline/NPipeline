namespace Sample_RouteNode.Models;

/// <summary>
///     Represents an order event used in the RouteNode sample.
/// </summary>
/// <param name="OrderId">The unique order identifier.</param>
/// <param name="CustomerId">The customer identifier.</param>
/// <param name="Country">The shipping country code.</param>
/// <param name="Amount">The order total amount.</param>
public sealed record OrderEvent(
    string OrderId,
    string CustomerId,
    string Country,
    decimal Amount);
