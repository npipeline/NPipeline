using System.Globalization;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_ComplexDataTransformations.Nodes;

/// <summary>
///     Source node that generates customer data for the complex data transformations pipeline.
///     This node creates realistic e-commerce customer data with various countries and registration dates.
/// </summary>
public class CustomerSource : SourceNode<Customer>
{
    /// <summary>
    ///     Generates a collection of customers with realistic e-commerce data.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the generated customers.</returns>
    public override IDataPipe<Customer> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Generating customer data...");

        var random = new Random(42); // Fixed seed for reproducible results
        var customers = new List<Customer>();
        var baseDate = DateTime.UtcNow.AddDays(-365);

        var firstNames = new[] { "John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa", "James", "Mary" };
        var lastNames = new[] { "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez" };
        var countries = new[] { "USA", "Canada", "UK", "Germany", "France", "Australia", "Japan", "Brazil", "India", "Singapore" };

        // Generate 10 customers
        for (var i = 1; i <= 10; i++)
        {
            var firstName = firstNames[random.Next(firstNames.Length)];
            var lastName = lastNames[random.Next(lastNames.Length)];
            var name = $"{firstName} {lastName}";
            var email = $"{firstName.ToLower(CultureInfo.InvariantCulture)}.{lastName.ToLower(CultureInfo.InvariantCulture)}@example.com";
            var country = countries[random.Next(countries.Length)];
            var registrationDate = baseDate.AddDays(random.Next(0, 300));

            customers.Add(new Customer(i, name, email, country, registrationDate));
        }

        Console.WriteLine($"Generated {customers.Count} customers");
        return new InMemoryDataPipe<Customer>(customers, "CustomerSource");
    }
}
