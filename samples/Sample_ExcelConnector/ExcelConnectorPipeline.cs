using NPipeline.Connectors;
using NPipeline.Connectors.Excel;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using Sample_ExcelConnector.Nodes;

namespace Sample_ExcelConnector;

/// <summary>
///     Excel Connector pipeline that demonstrates reading from an Excel file, validating and transforming data,
///     and writing processed data to another Excel file.
/// </summary>
/// <remarks>
///     This pipeline implements a complete Excel processing workflow:
///     1. ExcelSourceNode reads customer data from a source Excel file
///     2. ValidationTransform validates customer records and filters invalid ones
///     3. DataTransform enriches and normalizes customer data
///     4. ExcelSinkNode writes the processed data to a target Excel file
/// </remarks>
public class ExcelConnectorPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <remarks>
    ///     This method creates a simple Excel processing pipeline:
    ///     ExcelSourceNode -> ValidationTransform -> DataTransform -> ExcelSinkNode
    ///     The pipeline reads customer data from the input Excel file, validates and transforms it,
    ///     then writes the processed records to an output Excel file.
    ///     Note: The resolver parameter is optional for local files - the ExcelSourceNode and ExcelSinkNode
    ///     automatically create a default file system resolver if none is provided.
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Define paths for source and output files
        var sourcePath = GetSourcePath();
        var targetPath = GetTargetPath();

        // Create the Excel source node - reads customer data from the input file
        // Resolver is optional; defaults to file system provider for local files
        var sourceNode = new ExcelSourceNode<Customer>(
            StorageUri.FromFilePath(sourcePath),
            row => new Customer
            {
                Id = row.Get<int>("Id") ?? 0,
                FirstName = row.Get<string>("FirstName") ?? string.Empty,
                LastName = row.Get<string>("LastName") ?? string.Empty,
                Email = row.Get<string>("Email") ?? string.Empty,
                Age = row.Get<int>("Age") ?? 0,
                RegistrationDate = row.Get<DateTime>("RegistrationDate") ?? default,
                Country = row.Get<string>("Country") ?? string.Empty,
                AccountBalance = row.Get<decimal>("AccountBalance") ?? 0m,
                IsPremiumMember = row.Get<bool>("IsPremiumMember") ?? false,
                DiscountPercentage = row.Get<double>("DiscountPercentage") ?? 0d,
                LoyaltyPoints = row.Get<long>("LoyaltyPoints") ?? 0L,
            });
        var source = builder.AddSource(sourceNode, "excel-source");

        // Add validation transform to filter invalid records
        var validation = builder.AddTransform<ValidationTransform, Customer, Customer>("validation-transform");

        // Add data transform to enrich and normalize records
        var transform = builder.AddTransform<DataTransform, Customer, Customer>("data-transform");

        // Create the Excel sink node - writes processed customer data to the output file
        var sinkNode = new ExcelSinkNode<Customer>(StorageUri.FromFilePath(targetPath));
        var sink = builder.AddSink(sinkNode, "excel-sink");

        // Connect nodes in sequence: source -> validation -> transform -> sink
        builder.Connect(source, validation);
        builder.Connect(validation, transform);
        builder.Connect(transform, sink);

        // Log pipeline configuration
        var logger = context.LoggerFactory.CreateLogger("ExcelConnectorPipeline");
        logger.Log(LogLevel.Information, "Excel pipeline configured: {SourcePath} -> {TargetPath}", sourcePath, targetPath);
    }

    /// <summary>
    ///     Gets the source Excel file path.
    /// </summary>
    private static string GetSourcePath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "customers.xlsx");
    }

    /// <summary>
    ///     Gets the target Excel file path for processed data.
    /// </summary>
    private static string GetTargetPath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "processed_customers.xlsx");
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
        return @"Excel Connector Pipeline Sample:

This sample demonstrates Excel data processing with NPipeline:

WHAT IT DOES:
- Reads customer records from an Excel file (customers.xlsx)
- Validates each record and filters out invalid ones
- Transforms and enriches the valid records
- Writes the processed records to a new Excel file (processed_customers.xlsx)

PIPELINE FLOW:
ExcelSourceNode (read)
  → ValidationTransform (filter invalid records)
    → DataTransform (enrich/normalize)
      → ExcelSinkNode (write)

KEY FEATURES:
- Simple file-based Excel processing using StorageUri
- Data validation with filtering
- Data transformation and enrichment
- Built-in error handling and logging
- No complex configuration needed - just specify the file paths

GETTING STARTED:
The pipeline is straightforward - create node instances with file paths,
add them to the builder, and connect them. The ExcelSourceNode and
ExcelSinkNode automatically handle the file system interactions.

This is one of the simplest ways to process Excel files in NPipeline!";
    }
}
