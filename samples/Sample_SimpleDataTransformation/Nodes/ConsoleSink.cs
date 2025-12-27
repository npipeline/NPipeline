using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_SimpleDataTransformation.Nodes;

/// <summary>
///     Sink node that outputs processed EnrichedPerson objects to the console.
///     This node demonstrates how to create a sink that consumes enriched data
///     by inheriting directly from SinkNode&lt;EnrichedPerson&gt;.
/// </summary>
public class ConsoleSink : SinkNode<EnrichedPerson>
{
    /// <summary>
    ///     Processes the input EnrichedPerson objects by writing them to the console with formatting.
    /// </summary>
    /// <param name="input">The data pipe containing input EnrichedPerson objects to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the sink execution.</returns>
    public override async Task ExecuteAsync(IDataPipe<EnrichedPerson> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Starting to process enriched person records in ConsoleSink");
        Console.WriteLine();

        var recordCount = 0;
        var validEmailCount = 0;

        // Print header
        Console.WriteLine("=== FINAL TRANSFORMED DATA ===");
        Console.WriteLine();
        Console.WriteLine("ID | Name              | Age | City            | Country | Age Category     | Valid Email");
        Console.WriteLine("---|-------------------|-----|-----------------|---------|------------------|------------");

        // Use await foreach to consume all messages from the input pipe
        // This is the standard pattern for consuming data in sink nodes
        await foreach (var person in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Skip placeholder items (those with ID 0)
            if (person.Id == 0)
                continue;

            recordCount++;

            if (person.IsValidEmail)
                validEmailCount++;

            // Format and display the enriched person data
            var name = $"{person.FirstName} {person.LastName}".PadRight(17);
            var city = person.City.PadRight(15);
            var country = person.Country.PadRight(7);
            var ageCategory = person.AgeCategory.PadRight(16);

            Console.WriteLine($"{person.Id:D2} | {name} | {person.Age:D3} | {city} | {country} | {ageCategory} | {person.IsValidEmail}");
        }

        Console.WriteLine();
        Console.WriteLine("=== SUMMARY ===");
        Console.WriteLine($"Total records processed: {recordCount}");
        Console.WriteLine($"Records with valid emails: {validEmailCount}");
        Console.WriteLine($"Records with invalid emails: {recordCount - validEmailCount}");
        Console.WriteLine($"Success rate: {(recordCount > 0 ? validEmailCount * 100.0 / recordCount : 0):F1}%");

        Console.WriteLine();
        Console.WriteLine("ConsoleSink completed processing all enriched person records.");
    }
}
