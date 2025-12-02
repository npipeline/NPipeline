using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_KeyedJoinNode.Nodes;

/// <summary>
///     Source node that generates sample customer data for demonstrating keyed join functionality.
///     This node creates a stream of customers with different tiers and registration dates.
/// </summary>
public class CustomerSource : SourceNode<Customer>
{
    private readonly TimeSpan _delayBetweenCustomers;
    private readonly ILogger<CustomerSource>? _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CustomerSource" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    /// <param name="delayBetweenCustomers">Delay between generating customers to simulate real-time data.</param>
    public CustomerSource(ILogger<CustomerSource>? logger = null, TimeSpan? delayBetweenCustomers = null)
    {
        _logger = logger;
        _delayBetweenCustomers = delayBetweenCustomers ?? TimeSpan.FromMilliseconds(200);
    }

    /// <inheritdoc />
    public override IDataPipe<Customer> Execute(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger?.LogInformation("CustomerSource: Starting to generate customers");

        // Generate sample customers with different tiers
        var customers = new List<Customer>
        {
            new(1, "Alice Johnson", "alice.johnson@example.com", DateTime.UtcNow.AddDays(-365), "Gold"),
            new(2, "Bob Smith", "bob.smith@example.com", DateTime.UtcNow.AddDays(-200), "Silver"),
            new(3, "Charlie Brown", "charlie.brown@example.com", DateTime.UtcNow.AddDays(-100), "Bronze"),
            new(4, "Diana Prince", "diana.prince@example.com", DateTime.UtcNow.AddDays(-500), "Platinum"),
            new(5, "Eve Wilson", "eve.wilson@example.com", DateTime.UtcNow.AddDays(-150), "Gold"),
            new(6, "Frank Miller", "frank.miller@example.com", DateTime.UtcNow.AddDays(-80), "Silver"),
            new(7, "Grace Lee", "grace.lee@example.com", DateTime.UtcNow.AddDays(-300), "Gold"),
            new(8, "Henry Ford", "henry.ford@example.com", DateTime.UtcNow.AddDays(-50), "Bronze"),
            new(9, "Iris West", "iris.west@example.com", DateTime.UtcNow.AddDays(-400), "Platinum"),
            new(10, "Jack Ryan", "jack.ryan@example.com", DateTime.UtcNow.AddDays(-120), "Silver"),

            // Note: Customer 11 is intentionally missing to demonstrate unmatched orders
            new(12, "Linda Carter", "linda.carter@example.com", DateTime.UtcNow.AddDays(-250), "Gold"),
        };

        _logger?.LogInformation("CustomerSource: Finished generating {Count} customers", customers.Count);

        return new InMemoryDataPipe<Customer>(customers, "CustomerSource");
    }
}
