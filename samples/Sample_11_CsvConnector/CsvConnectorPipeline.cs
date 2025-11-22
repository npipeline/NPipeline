using NPipeline.Connectors;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Csv;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using Sample_11_CsvConnector.Nodes;

namespace Sample_11_CsvConnector;

/// <summary>
///     Custom node factory for creating CSV nodes with proper parameter resolution.
/// </summary>
public class CsvNodeFactory
{
    /// <summary>
    ///     Creates a CsvSourceNode with the correct constructor based on available parameters.
    /// </summary>
    public static CsvSourceNode<Customer> CreateCsvSourceNode(PipelineContext context)
    {
        var sourceUri = context.Parameters.TryGetValue("CsvSourceUri", out var uriObj) && uriObj is StorageUri uri
            ? uri
            : StorageUri.FromFilePath(CsvConnectorPipeline.GetSourcePath(context));

        // Try to get resolver from context, otherwise use default
        var storageResolver = context.Properties.TryGetValue("StorageResolver", out var resolverObj) && resolverObj is IStorageResolver resolver
            ? resolver
            : StorageProviderFactory.CreateResolver();

        return new CsvSourceNode<Customer>(sourceUri, storageResolver);
    }

    /// <summary>
    ///     Creates a CsvSinkNode with the correct constructor based on available parameters.
    /// </summary>
    public static CsvSinkNode<Customer> CreateCsvSinkNode(PipelineContext context)
    {
        var sinkUri = context.Parameters.TryGetValue("CsvSinkUri", out var uriObj) && uriObj is StorageUri uri
            ? uri
            : StorageUri.FromFilePath(CsvConnectorPipeline.GetTargetPath(context));

        // Try to get resolver from context, otherwise use default
        var storageResolver = context.Properties.TryGetValue("StorageResolver", out var resolverObj) && resolverObj is IStorageResolver resolver
            ? resolver
            : StorageProviderFactory.CreateResolver();

        return new CsvSinkNode<Customer>(sinkUri, storageResolver);
    }
}

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
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a linear CSV processing pipeline:
    ///     CsvSourceNode -> ValidationTransform -> DataTransform -> CsvSinkNode
    ///     The pipeline processes customer data through these stages:
    ///     1. Source reads customer records from the input CSV file
    ///     2. Validation validates each record and filters out invalid ones
    ///     3. Transform enriches and normalizes the valid records
    ///     4. Sink writes the processed records to the output CSV file
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Store storage resolver in context for nodes to use
        if (!context.Properties.ContainsKey("StorageResolver"))
            context.Properties["StorageResolver"] = StorageProviderFactory.CreateResolver();

        // Add the CSV source node that reads customer data
        var source = builder.AddSource<CsvSourceNode<Customer>, Customer>("csv-source");

        // Add the validation transform node
        var validation = builder.AddTransform<ValidationTransform, Customer, Customer>("validation-transform");

        // Add the data transform node
        var transform = builder.AddTransform<DataTransform, Customer, Customer>("data-transform");

        // Add the CSV sink node that writes processed customer data
        var sink = builder.AddSink<CsvSinkNode<Customer>, Customer>("csv-sink");

        // Register pre-configured node instances to resolve constructor ambiguity
        _ = builder.AddPreconfiguredNodeInstance(source.Id, CsvNodeFactory.CreateCsvSourceNode(context));
        _ = builder.AddPreconfiguredNodeInstance(sink.Id, CsvNodeFactory.CreateCsvSinkNode(context));

        // Connect the nodes in a linear flow: source -> validation -> transform -> sink
        _ = builder.Connect(source, validation);
        _ = builder.Connect(validation, transform);
        _ = builder.Connect(transform, sink);

        // Log the pipeline configuration
        var logger = context.LoggerFactory.CreateLogger("CsvConnectorPipeline");

        logger.Log(
            LogLevel.Information,
            "CSV Connector Pipeline configured: {SourcePath} -> {TargetPath}",
            GetSourcePath(context),
            GetTargetPath(context));
    }

    /// <summary>
    ///     Gets the source CSV file path from context parameters or uses the default.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The source CSV file path.</returns>
    public static string GetSourcePath(PipelineContext context)
    {
        if (context.Parameters.TryGetValue("SourcePath", out var sourcePathObj) &&
            sourcePathObj is string sourcePath &&
            !string.IsNullOrEmpty(sourcePath))
            return sourcePath;

        // Get the project directory by navigating up from the current directory
        // This handles running from both IDE (project directory) and command line (build output directory)
        var currentDir = Directory.GetCurrentDirectory();
        var projectDir = FindProjectDirectory(currentDir);

        // Default to the sample customers.csv file in the project's Data directory
        return Path.Combine(projectDir, "Data", "customers.csv");
    }

    /// <summary>
    ///     Gets the target CSV file path from context parameters or uses the default.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The target CSV file path.</returns>
    public static string GetTargetPath(PipelineContext context)
    {
        if (context.Parameters.TryGetValue("TargetPath", out var targetPathObj) &&
            targetPathObj is string targetPath &&
            !string.IsNullOrEmpty(targetPath))
            return targetPath;

        // Get the project directory by navigating up from the current directory
        // This handles running from both IDE (project directory) and command line (build output directory)
        var currentDir = Directory.GetCurrentDirectory();
        var projectDir = FindProjectDirectory(currentDir);

        // Default to processed_customers.csv in the project's Data directory
        return Path.Combine(projectDir, "Data", "processed_customers.csv");
    }

    /// <summary>
    ///     Finds the project directory by looking for the .csproj file.
    /// </summary>
    /// <param name="startDirectory">The directory to start searching from.</param>
    /// <returns>The project directory path.</returns>
    private static string FindProjectDirectory(string startDirectory)
    {
        var directory = new DirectoryInfo(startDirectory);

        // Navigate up the directory tree looking for the .csproj file
        while (directory != null)
        {
            var csprojFile = directory.GetFiles("*.csproj").FirstOrDefault();

            if (csprojFile != null)
                return directory.FullName;

            directory = directory.Parent;
        }

        // If we can't find the project file, fall back to the original directory
        return startDirectory;
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"CSV Connector Pipeline Sample:

This sample demonstrates comprehensive CSV data processing using NPipeline:
- Reading CSV data using CsvSourceNode
- Validating CSV records and filtering invalid data
- Transforming and enriching CSV data
- Writing processed data to a new CSV file using CsvSinkNode
- Proper error handling for CSV operations

The pipeline flow:
1. CsvSourceNode reads customer records from the input CSV file
2. ValidationTransform validates each record (ID, email, age, etc.) and filters invalid ones
3. DataTransform enriches and normalizes the valid records (name formatting, country normalization)
4. CsvSinkNode writes the processed records to the output CSV file

Key features demonstrated:
- StorageUri abstraction for file system access
- Configurable CSV processing with CsvHelper
- Data validation and filtering patterns
- Data transformation and enrichment patterns
- Error handling and logging throughout the pipeline
- Flexible configuration through pipeline parameters

This implementation follows the IPipelineDefinition pattern, which provides:
- Reusable pipeline definitions
- Proper node isolation between executions
- Type-safe node connections
- Clear separation of pipeline structure from execution logic";
    }
}
