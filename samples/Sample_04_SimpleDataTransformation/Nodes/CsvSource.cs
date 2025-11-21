using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_04_SimpleDataTransformation;
using StringReader = System.IO.StringReader;

namespace Sample_04_SimpleDataTransformation.Nodes;

/// <summary>
///     Source node that reads CSV data and converts to Person objects.
///     This node demonstrates how to create a source that processes CSV data
///     by inheriting directly from SourceNode&lt;Person&gt;.
/// </summary>
public class CsvSource : SourceNode<Person>
{
    /// <summary>
    ///     Generates a collection of Person objects from CSV data.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the Person objects.</returns>
    public override IDataPipe<Person> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Reading CSV data and converting to Person objects");

        // Sample CSV data - in a real scenario this would come from a file
        var csvData = @"Id,FirstName,LastName,Age,Email,City
1,John,Doe,25,john.doe@example.com,New York
2,Jane,Smith,30,jane.smith@example.com,Los Angeles
3,Bob,Johnson,17,bob.johnson@example.com,Chicago
4,Alice,Williams,28,alice.williams@example.com,Houston
5,Charlie,Brown,35,charlie.brown@example.com,Phoenix
6,Diana,Prince,29,diana.prince@example.com,Philadelphia
7,Edward,Norton,42,edward.norton@example.com,San Antonio
8,Fiona,Apple,16,fiona.apple@example.com,San Diego
9,George,Clooney,55,george.clooney@example.com,Dallas
10,Helen,Miller,23,helen.miller@example.com,San Jose";

        var people = new List<Person>();

        try
        {
            using var reader = new StringReader(csvData);
            using var csv = new CsvReader(reader, new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                HeaderValidated = null,
                MissingFieldFound = null
            });

            var records = csv.GetRecords<Person>();
            people.AddRange(records);

            Console.WriteLine($"Successfully parsed {people.Count} Person records from CSV data");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error parsing CSV data: {ex.Message}");
            throw;
        }

        // Return a ListDataPipe containing our Person objects
        return new ListDataPipe<Person>(people, "CsvSource");
    }
}