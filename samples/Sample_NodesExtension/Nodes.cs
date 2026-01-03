using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Extensions.Nodes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_NodesExtension;

/// <summary>
///     Source node that generates sample customer data for processing.
/// </summary>
public class CustomerDataSource : SourceNode<CustomerRecord>
{
    private static readonly List<CustomerRecord> SampleData = new()
    {
        new CustomerRecord
        {
            Id = 1,
            Name = "  John Doe  ",
            Email = "JOHN.DOE@EXAMPLE.COM",
            Phone = "555-1234",
            Age = 35,
            AccountBalance = 5000,
            CreatedDate = DateTime.SpecifyKind(new DateTime(2024, 1, 15, 10, 30, 0), DateTimeKind.Local),
            Tags = ["vip", "online", "active"],
        },
        new CustomerRecord
        {
            Id = 2,
            Name = "  Jane Smith  ",
            Email = "jane.smith@example.com",
            Phone = "555-5678  ",
            Age = 28,
            AccountBalance = 7500,
            CreatedDate = DateTime.SpecifyKind(new DateTime(2024, 2, 20, 14, 45, 0), DateTimeKind.Local),
            Tags = ["premium", "inactive"],
        },
        new CustomerRecord
        {
            Id = 3,
            Name = "  Bob Wilson  ",
            Email = "bob.wilson@example.com",
            Phone = "555-9999",
            Age = 45,
            AccountBalance = 9200,
            CreatedDate = DateTime.SpecifyKind(new DateTime(2023, 12, 10, 9, 0, 0), DateTimeKind.Local),
            Tags = ["loyal", "business"],
        },
        new CustomerRecord
        {
            Id = 4,
            Name = "  Alice Brown  ",
            Email = "ALICE@EXAMPLE.COM",
            Phone = "555-4321",
            Age = 31,
            AccountBalance = 12000,
            CreatedDate = DateTime.SpecifyKind(new DateTime(2023, 11, 30, 16, 20, 0), DateTimeKind.Local),
            Tags = ["vip", "loyal", "premium"],
        },
    };

    public override IDataPipe<CustomerRecord> Initialize(
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        return new InMemoryDataPipe<CustomerRecord>(SampleData, "CustomerDataSource");
    }
}

/// <summary>
///     Custom validation node for customer records.
///     Demonstrates how to create domain-specific validation rules.
/// </summary>
public class CustomerValidator : ValidationNode<CustomerRecord>
{
    public CustomerValidator()
    {
        // Validate Name
        Register(
            c => c.Name,
            name => !string.IsNullOrWhiteSpace(name),
            "NameRequired",
            _ => "Customer name cannot be empty");

        // Validate Email format (basic check)
        Register(
            c => c.Email,
            email => !string.IsNullOrEmpty(email) && email.Contains('@'),
            "EmailFormat",
            _ => "Email must be in valid format (contains @)");

        // Validate Age range
        Register(
            c => c.Age,
            age => age >= 18 && age <= 120,
            "AgeRange",
            age => $"Age must be between 18 and 120, got {age}");

        // Validate Account Balance is non-negative
        Register(
            c => c.AccountBalance,
            balance => balance >= 0,
            "PositiveBalance",
            balance => $"Account balance must be non-negative, got {balance:C}");
    }
}

/// <summary>
///     Sink node that outputs processed customer records to the console.
/// </summary>
public class CustomerConsoleSink : SinkNode<CustomerRecord>
{
    public override async Task ExecuteAsync(
        IDataPipe<CustomerRecord> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var count = 0;

        // Use await foreach to consume all customers from the input pipe
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            count++;
            Console.WriteLine($"[{count}] {item}");
        }
    }
}
