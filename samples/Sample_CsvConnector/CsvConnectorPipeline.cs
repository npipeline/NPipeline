using NPipeline.Connectors;
using NPipeline.Connectors.Csv;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using Sample_CsvConnector.Nodes;

namespace Sample_CsvConnector;

/// <summary>
///     CSV Connector pipeline that demonstrates reading from a CSV file, validating and transforming data,
///     and writing processed data to another CSV file.
/// </summary>
/// <remarks>
///     This pipeline implements a complete CSV processing workflow:
///     1. CsvSourceNode reads customer data from a source CSV file
///     2. ValidationTransform validates customer records and filters invalid ones
///     3. DataTransform enriches and normalizes customer data
///     4. CsvSinkNode writes the processed data to a target CSV file
/// </remarks>
public class CsvConnectorPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <remarks>
    ///     This method creates a simple CSV processing pipeline:
    ///     CsvSourceNode -> ValidationTransform -> DataTransform -> CsvSinkNode
    ///     The pipeline reads customer data from the input CSV file, validates and transforms it,
    ///     then writes the processed records to an output CSV file.
    ///     Note: The resolver parameter is optional for local files - the CsvSourceNode and CsvSinkNode
    ///     automatically create a default file system resolver if none is provided.
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Define paths for source and output files
        var sourcePath = GetSourcePath();
        var targetPath = GetTargetPath();

        // OPTION 1: Traditional approach with manual mapper function
        // This gives you full control over the mapping logic
        var sourceNodeManual = new CsvSourceNode<Customer>(
            StorageUri.FromFilePath(sourcePath),
            row => new Customer
            {
                Id = row.Get("Id", 0),
                FirstName = row.Get("FirstName", string.Empty),
                LastName = row.Get("LastName", string.Empty),
                Email = row.Get("Email", string.Empty),
                Age = row.Get("Age", 0),
                RegistrationDate = row.Get("RegistrationDate", default(DateTime)),
                Country = row.Get("Country", string.Empty),
            });

        // OPTION 2: Attribute-based mapping (recommended for most scenarios)
        // When Customer class has CsvColumn attributes, the mapper is built automatically
        // using CsvMapperBuilder<T> with compiled expression tree delegates for optimal performance
        // Uncomment the line below to use attribute-based mapping instead:
        // var sourceNode = new CsvSourceNode<Customer>(StorageUri.FromFilePath(sourcePath));

        // For this sample, we use the manual mapper to demonstrate both approaches
        var sourceNode = sourceNodeManual;
        var source = builder.AddSource(sourceNode, "csv-source");

        // Add validation transform to filter invalid records
        var validation = builder.AddTransform<ValidationTransform, Customer, Customer>("validation-transform");

        // Add data transform to enrich and normalize records
        var transform = builder.AddTransform<DataTransform, Customer, Customer>("data-transform");

        // Create the CSV sink node - writes processed customer data to the output file
        // With attribute-based mapping, columns are written in the order defined by attributes
        // Computed properties marked with [CsvIgnore] are automatically excluded
        var sinkNode = new CsvSinkNode<Customer>(StorageUri.FromFilePath(targetPath));
        var sink = builder.AddSink(sinkNode, "csv-sink");

        // Connect nodes in sequence: source -> validation -> transform -> sink
        builder.Connect(source, validation);
        builder.Connect(validation, transform);
        builder.Connect(transform, sink);

        // Log pipeline configuration
        var logger = context.LoggerFactory.CreateLogger("CsvConnectorPipeline");
        logger.Log(LogLevel.Information, "CSV pipeline configured: {SourcePath} -> {TargetPath}", sourcePath, targetPath);
    }

    /// <summary>
    ///     Gets the source CSV file path.
    /// </summary>
    private static string GetSourcePath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "customers.csv");
    }

    /// <summary>
    ///     Gets the target CSV file path for processed data.
    /// </summary>
    private static string GetTargetPath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "processed_customers.csv");
    }

    /// <summary>
    ///     Finds the project directory by looking for the .csproj file.
    /// </summary>
    private static string FindProjectDirectory(string startDirectory)
    {
        var directory = new DirectoryInfo(startDirectory);

        // Navigate up the directory tree looking for the .csproj file
        while (directory != null)
        {
            if (directory.GetFiles("*.csproj").FirstOrDefault() != null)
                return directory.FullName;

            directory = directory.Parent;
        }

        return startDirectory;
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    public static string GetDescription()
    {
        return @"CSV Connector Pipeline Sample:

This sample demonstrates CSV data processing with NPipeline:

WHAT IT DOES:
- Reads customer records from a CSV file (customers.csv)
- Validates each record and filters out invalid ones
- Transforms and enriches the valid records
- Writes the processed records to a new CSV file (processed_customers.csv)

PIPELINE FLOW:
CsvSourceNode (read)
  → ValidationTransform (filter invalid records)
    → DataTransform (enrich/normalize)
      → CsvSinkNode (write)

KEY FEATURES:
- Simple file-based CSV processing using StorageUri
- Data validation with filtering
- Data transformation and enrichment
- Built-in error handling and logging
- No complex configuration needed - just specify the file paths

GETTING STARTED:
The pipeline is straightforward - create node instances with file paths,
add them to the builder, and connect them. The CsvSourceNode and
CsvSinkNode automatically handle the file system interactions.

This is one of the simplest ways to process CSV files in NPipeline!";
    }
}
