using NPipeline.Connectors;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Excel;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using Sample_ExcelConnector.Nodes;

namespace Sample_ExcelConnector;

/// <summary>
///     Custom node factory for creating Excel nodes with proper parameter resolution.
/// </summary>
public class ExcelNodeFactory
{
    /// <summary>
    ///     Creates an ExcelSourceNode with the correct constructor based on available parameters.
    /// </summary>
    public static ExcelSourceNode<Customer> CreateExcelSourceNode(PipelineContext context)
    {
        var sourceUri = context.Parameters.TryGetValue("ExcelSourceUri", out var uriObj) && uriObj is StorageUri uri
            ? uri
            : StorageUri.FromFilePath(ExcelConnectorPipeline.GetSourcePath(context));

        // Try to get resolver from context, otherwise use default
        var storageResolver = context.Properties.TryGetValue("StorageResolver", out var resolverObj) && resolverObj is IStorageResolver resolver
            ? resolver
            : StorageProviderFactory.CreateResolver().Resolver;

        return new ExcelSourceNode<Customer>(sourceUri, storageResolver);
    }

    /// <summary>
    ///     Creates an ExcelSinkNode with the correct constructor based on available parameters.
    /// </summary>
    public static ExcelSinkNode<Customer> CreateExcelSinkNode(PipelineContext context)
    {
        var sinkUri = context.Parameters.TryGetValue("ExcelSinkUri", out var uriObj) && uriObj is StorageUri uri
            ? uri
            : StorageUri.FromFilePath(ExcelConnectorPipeline.GetTargetPath(context));

        // Try to get resolver from context, otherwise use default
        var storageResolver = context.Properties.TryGetValue("StorageResolver", out var resolverObj) && resolverObj is IStorageResolver resolver
            ? resolver
            : StorageProviderFactory.CreateResolver().Resolver;

        return new ExcelSinkNode<Customer>(sinkUri, storageResolver);
    }
}

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
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a linear Excel processing pipeline:
    ///     ExcelSourceNode -> ValidationTransform -> DataTransform -> ExcelSinkNode
    ///     The pipeline processes customer data through these stages:
    ///     1. Source reads customer records from the input Excel file
    ///     2. Validation validates each record and filters out invalid ones
    ///     3. Transform enriches and normalizes the valid records
    ///     4. Sink writes the processed records to the output Excel file
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Store storage resolver in context for nodes to use
        if (!context.Properties.ContainsKey("StorageResolver"))
            context.Properties["StorageResolver"] = StorageProviderFactory.CreateResolver().Resolver;

        // Add the Excel source node that reads customer data
        var source = builder.AddSource<ExcelSourceNode<Customer>, Customer>("excel-source");

        // Add the validation transform node
        var validation = builder.AddTransform<ValidationTransform, Customer, Customer>("validation-transform");

        // Add the data transform node
        var transform = builder.AddTransform<DataTransform, Customer, Customer>("data-transform");

        // Add the Excel sink node that writes processed customer data
        var sink = builder.AddSink<ExcelSinkNode<Customer>, Customer>("excel-sink");

        // Register pre-configured node instances to resolve constructor ambiguity
        _ = builder.AddPreconfiguredNodeInstance(source.Id, ExcelNodeFactory.CreateExcelSourceNode(context));
        _ = builder.AddPreconfiguredNodeInstance(sink.Id, ExcelNodeFactory.CreateExcelSinkNode(context));

        // Connect the nodes in a linear flow: source -> validation -> transform -> sink
        _ = builder.Connect(source, validation);
        _ = builder.Connect(validation, transform);
        _ = builder.Connect(transform, sink);

        // Log the pipeline configuration
        var logger = context.LoggerFactory.CreateLogger("ExcelConnectorPipeline");

        logger.Log(
            LogLevel.Information,
            "Excel Connector Pipeline configured: {SourcePath} -> {TargetPath}",
            GetSourcePath(context),
            GetTargetPath(context));
    }

    /// <summary>
    ///     Gets the source Excel file path from context parameters or uses the default.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The source Excel file path.</returns>
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

        // Default to the sample customers.xlsx file in the project's Data directory
        return Path.Combine(projectDir, "Data", "customers.xlsx");
    }

    /// <summary>
    ///     Gets the target Excel file path from context parameters or uses the default.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The target Excel file path.</returns>
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

        // Default to processed_customers.xlsx in the project's Data directory
        return Path.Combine(projectDir, "Data", "processed_customers.xlsx");
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
        return @"Excel Connector Pipeline Sample:

This sample demonstrates comprehensive Excel data processing using NPipeline:
- Reading Excel data using ExcelSourceNode
- Validating Excel records and filtering invalid data
- Transforming and enriching Excel data
- Writing processed data to a new Excel file using ExcelSinkNode
- Proper error handling for Excel operations

The pipeline flow:
1. ExcelSourceNode reads customer records from the input Excel file
2. ValidationTransform validates each record (ID, email, age, balance, etc.) and filters invalid ones
3. DataTransform enriches and normalizes the valid records (name formatting, country normalization)
4. ExcelSinkNode writes the processed records to the output Excel file

Key features demonstrated:
- StorageUri abstraction for file system access
- Configurable Excel processing with ExcelDataReader and DocumentFormat.OpenXml
- Data validation and filtering patterns
- Data transformation and enrichment patterns
- Error handling and logging throughout the pipeline
- Flexible configuration through pipeline parameters
- Support for various data types (string, int, decimal, DateTime, bool, double, long)

This implementation follows the IPipelineDefinition pattern, which provides:
- Reusable pipeline definitions
- Proper node isolation between executions
- Type-safe node connections
- Clear separation of pipeline structure from execution logic";
    }
}
