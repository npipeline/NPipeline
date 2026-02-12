using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Json;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;
using Sample_JsonConnector.Nodes;

namespace Sample_JsonConnector;

/// <summary>
///     JSON Connector pipeline that demonstrates reading from JSON files, validating and transforming data,
///     and writing processed data to another JSON file.
/// </summary>
/// <remarks>
///     This pipeline implements a complete JSON processing workflow:
///     1. JsonSourceNode reads customer data from a source JSON file
///     2. ValidationTransform validates customer records and filters invalid ones
///     3. DataTransform enriches and normalizes customer data
///     4. JsonSinkNode writes the processed data to a target JSON file
///     
///     The pipeline demonstrates:
///     - Attribute-based mapping using Column attributes
///     - Different JSON formats (Array and NDJSON)
///     - Custom configuration options (naming policies, indented output)
///     - Error handling with RowErrorHandler
/// </remarks>
public class JsonConnectorPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <remarks>
    ///     This method creates a JSON processing pipeline with multiple scenarios:
    ///     Scenario 1: JSON Array format with attribute-based mapping
    ///     Scenario 2: NDJSON format with custom configuration
    ///     
    ///     The pipeline reads customer data from the input JSON file, validates and transforms it,
    ///     then writes the processed records to an output JSON file.
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Define paths for source and output files
        var sourcePath = GetSourcePath();
        var targetPath = GetTargetPath();
        var ndjsonSourcePath = GetNdjsonSourcePath();
        var ndjsonTargetPath = GetNdjsonTargetPath();

        // SCENARIO 1: JSON Array format with attribute-based mapping
        // The Customer class uses Column attributes for property mapping
        // The mapper is automatically built using JsonMapperBuilder<T>
        var jsonConfiguration = new JsonConfiguration
        {
            Format = JsonFormat.Array,
            PropertyNamingPolicy = JsonPropertyNamingPolicy.CamelCase,
            WriteIndented = true,
        };

        var sourceNode = new JsonSourceNode<Customer>(
            StorageUri.FromFilePath(sourcePath),
            configuration: jsonConfiguration);

        var source = builder.AddSource(sourceNode, "json-source");

        // Add validation transform to filter invalid records
        var validation = builder.AddTransform<ValidationTransform, Customer, Customer>("validation-transform");

        // Add data transform to enrich and normalize records
        var transform = builder.AddTransform<DataTransform, Customer, Customer>("data-transform");

        // Create the JSON sink node - writes processed customer data to the output file
        // With attribute-based mapping, properties are written based on Column attributes
        // Computed properties marked with [IgnoreColumn] are automatically excluded
        var sinkNode = new JsonSinkNode<Customer>(
            StorageUri.FromFilePath(targetPath),
            configuration: jsonConfiguration);

        var sink = builder.AddSink(sinkNode, "json-sink");

        // Connect nodes in sequence: source -> validation -> transform -> sink
        builder.Connect(source, validation);
        builder.Connect(validation, transform);
        builder.Connect(transform, sink);

        // Log pipeline configuration
        var logger = context.LoggerFactory.CreateLogger("JsonConnectorPipeline");
        logger.Log(LogLevel.Information, "JSON pipeline configured: {SourcePath} -> {TargetPath}", sourcePath, targetPath);
        logger.Log(LogLevel.Information, "JSON format: {Format}, Naming policy: {NamingPolicy}, Indented: {WriteIndented}",
            jsonConfiguration.Format, jsonConfiguration.PropertyNamingPolicy, jsonConfiguration.WriteIndented);

        // SCENARIO 2: NDJSON format with custom configuration
        // Uncomment the following code to demonstrate NDJSON processing
        /*
        var ndjsonConfiguration = new JsonConfiguration
        {
            Format = JsonFormat.NewlineDelimited,
            PropertyNamingPolicy = JsonPropertyNamingPolicy.CamelCase,
            WriteIndented = false,
        };

        var ndjsonSourceNode = new JsonSourceNode<Customer>(
            StorageUri.FromFilePath(ndjsonSourcePath),
            configuration: ndjsonConfiguration);

        var ndjsonSource = builder.AddSource(ndjsonSourceNode, "ndjson-source");

        var ndjsonValidation = builder.AddTransform<ValidationTransform, Customer, Customer>("ndjson-validation");
        var ndjsonTransform = builder.AddTransform<DataTransform, Customer, Customer>("ndjson-transform");

        var ndjsonSinkNode = new JsonSinkNode<Customer>(
            StorageUri.FromFilePath(ndjsonTargetPath),
            configuration: ndjsonConfiguration);

        var ndjsonSink = builder.AddSink(ndjsonSinkNode, "ndjson-sink");

        builder.Connect(ndjsonSource, ndjsonValidation);
        builder.Connect(ndjsonValidation, ndjsonTransform);
        builder.Connect(ndjsonTransform, ndjsonSink);

        logger.Log(LogLevel.Information, "NDJSON pipeline configured: {SourcePath} -> {TargetPath}", ndjsonSourcePath, ndjsonTargetPath);
        */
    }

    /// <summary>
    ///     Gets the source JSON array file path.
    /// </summary>
    private static string GetSourcePath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "customers.json");
    }

    /// <summary>
    ///     Gets the target JSON array file path for processed data.
    /// </summary>
    private static string GetTargetPath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "processed_customers.json");
    }

    /// <summary>
    ///     Gets the source NDJSON file path.
    /// </summary>
    private static string GetNdjsonSourcePath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "customers.ndjson");
    }

    /// <summary>
    ///     Gets the target NDJSON file path for processed data.
    /// </summary>
    private static string GetNdjsonTargetPath()
    {
        var projectDir = FindProjectDirectory(Directory.GetCurrentDirectory());
        return Path.Combine(projectDir, "Data", "processed_customers.ndjson");
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
        return @"JSON Connector Pipeline Sample:

This sample demonstrates JSON data processing with NPipeline:

WHAT IT DOES:
- Reads customer records from a JSON file (customers.json)
- Validates each record and filters out invalid ones
- Transforms and enriches the valid records
- Writes the processed records to a new JSON file (processed_customers.json)

PIPELINE FLOW:
JsonSourceNode (read)
  → ValidationTransform (filter invalid records)
    → DataTransform (enrich/normalize)
      → JsonSinkNode (write)

KEY FEATURES:
- Simple file-based JSON processing using StorageUri
- Attribute-based mapping using Column attributes
- Support for different JSON formats (Array and NDJSON)
- Custom configuration options (naming policies, indented output)
- Data validation with filtering
- Data transformation and enrichment
- Built-in error handling and logging
- No complex configuration needed - just specify the file paths

ATTRIBUTE-BASED MAPPING:
The Customer class uses Column attributes to map JSON properties:
- [Column(""id"")] maps to the ""id"" JSON property
- [Column(""firstName"")] maps to the ""firstName"" JSON property
- [IgnoreColumn] excludes computed properties from JSON output

JSON FORMATS:
- Array format: Traditional JSON array with all records in a single array
- NDJSON format: Newline-delimited JSON with one record per line

CONFIGURATION OPTIONS:
- Format: Choose between Array or NDJSON format
- PropertyNamingPolicy: CamelCase, SnakeCase, or KebabCase
- WriteIndented: Control JSON output formatting
- ErrorHandling: SkipRecord or ThrowException

GETTING STARTED:
The pipeline is straightforward - create node instances with file paths,
add them to the builder, and connect them. The JsonSourceNode and
JsonSinkNode automatically handle the file system interactions.

This is one of the simplest ways to process JSON files in NPipeline!";
    }
}
