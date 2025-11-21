using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_02_FileProcessing;

/// <summary>
///     Entry point for the File Processing Pipeline sample demonstrating file-based data processing with NPipeline.
///     This sample shows a complete file processing workflow with source, transform, and sink nodes.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: File Processing Pipeline ===");
        Console.WriteLine();

        // Configure file paths for the pipeline
        const string inputFile = "Data/sample.txt";
        const string outputFile = "Data/output.txt";

        try
        {
            // Create a sample input file for demonstration
            await CreateSampleInputFileAsync(inputFile);

            Console.WriteLine($"Using input file: {inputFile}");
            Console.WriteLine($"Using output file: {outputFile}");
            Console.WriteLine();

            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    // Add NPipeline services with assembly scanning
                    _ = services.AddNPipeline(Assembly.GetExecutingAssembly());
                })
                .Build();

            Console.WriteLine("Registered NPipeline services and scanned assemblies for nodes.");
            Console.WriteLine();

            // Display pipeline description
            Console.WriteLine("Pipeline Description:");
            Console.WriteLine(FileProcessingPipeline.GetDescription());
            Console.WriteLine();

            // Execute the pipeline using the DI container with file path parameters
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            // Create pipeline parameters with file paths
            var pipelineParameters = new Dictionary<string, object>
            {
                ["FilePath"] = inputFile,
                ["OutputFilePath"] = outputFile,
            };

            await host.Services.RunPipelineAsync<FileProcessingPipeline>(pipelineParameters);

            Console.WriteLine();
            Console.WriteLine("Pipeline execution completed successfully!");

            // Display output file contents
            if (File.Exists(outputFile))
            {
                Console.WriteLine();
                Console.WriteLine($"Output file contents ({outputFile}):");
                Console.WriteLine(new string('=', 50));

                await foreach (var line in File.ReadLinesAsync(outputFile))
                {
                    Console.WriteLine(line);
                }

                Console.WriteLine(new string('=', 50));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error executing pipeline: {ex.Message}");
            Console.WriteLine();
            Console.WriteLine("Full error details:");
            Console.WriteLine(ex.ToString());
            Environment.ExitCode = 1;
        }
    }

    /// <summary>
    ///     Creates a sample input file for demonstration purposes.
    /// </summary>
    /// <param name="filePath">The path to the input file to create.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private static async Task CreateSampleInputFileAsync(string filePath)
    {
        // Ensure the directory exists
        var directory = Path.GetDirectoryName(filePath);

        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
            Console.WriteLine($"Created directory: {directory}");
        }

        if (File.Exists(filePath))
        {
            Console.WriteLine($"Input file '{filePath}' already exists, using existing file.");
            return;
        }

        Console.WriteLine($"Creating comprehensive sample input file: {filePath}");

        var sampleLines = new[]
        {
            "Welcome to the NPipeline File Processing Sample",
            "This file contains various types of content for processing",
            "123456789",
            "Mixed content with numbers 42 and text",
            "UPPERCASE LINE",
            "lowercase line",
            "MiXeD CaSe LiNe",
            "Line with special characters: !@#$%^&*()",
            "Line with punctuation: Hello, world! How are you today?",
            "Line with quotes: \"This is a quoted string\" and 'single quotes'",
            "Line with tabs	and	multiple	tab	characters",
            "Line with   multiple   spaces",
            "Line with trailing spaces   ",
            "   Line with leading spaces",
            "Line with both leading and trailing spaces   ",
            "",
            "Empty line above this one",
            "",
            "Line with URL: https://github.com/NPipeline/NPipeline",
            "Line with email: user@example.com",
            "Line with JSON: {\"key\": \"value\", \"number\": 123}",
            "Line with XML: <element attribute=\"value\">Content</element>",
            "Line with CSV: Name,Age,City - John,30,New York",
            "Line with date: 2023-12-25",
            "Line with time: 14:30:45",
            "Line with datetime: 2023-12-25T14:30:45Z",
            "Line with UUID: 550e8400-e29b-41d4-a716-446655440000",
            "Line with hexadecimal: 0xDEADBEEF",
            "Line with binary: 0b10101010",
            "Line with scientific notation: 1.23e-4",
            "Line with currency: $1,234.56",
            "Line with percentage: 75.5%",
            "Line with phone number: +1 (555) 123-4567",
            "Line with IP address: 192.168.1.1",
            "Line with file path: /usr/local/bin/application.exe",
            "Line with Windows path: C:\\Program Files\\Application\\app.exe",
            "Line with regex pattern: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
            "Line with SQL: SELECT * FROM users WHERE active = 1;",
            "Line with HTML: <p>This is a <strong>paragraph</strong> with <em>formatting</em>.</p>",
            "Line with CSS: .class-name { color: #ff0000; font-size: 14px; }",
            "Line with JavaScript: function greet(name) { return `Hello, ${name}!`; }",
            "Line with C# code: public string GetName() => \"NPipeline\";",
            "Line with Python: def calculate_sum(a, b): return a + b",
            "Line with mathematical expression: (a + b) * (c - d) / 2.0",
            "Line with Greek letters: α β γ δ ε ζ η θ ι κ λ μ ν ξ ο π ρ σ τ υ φ χ ψ ω",
            "Line with emojis: 🚀 📊 💻 🔄 📁 📝 ✅",
            "Line with accented characters: café, naïve, résumé, piñata, jalapeño",
            "Line with Unicode symbols: ★ ☆ ♠ ♣ ♥ ♦ © ® ™",
            "Very long line that demonstrates how the pipeline handles lines with substantial content that might wrap in the console but should be processed as a single line unit without any splitting or truncation during the transformation process",
            "Line with tab-separated values: ID\tName\tAge\tDepartment",
            "1\tJohn Doe\t35\tEngineering",
            "2\tJane Smith\t28\tMarketing",
            "3\tBob Johnson\t42\tSales",
            "Line with JSON array: [1, 2, 3, {\"name\": \"item\", \"value\": 42}]",
            "Line with YAML: key: value\n  nested:\n    subkey: subvalue",
            "Line with environment variable: PATH=/usr/local/bin:/usr/bin:/bin",
            "Line with command line: npm install --save-dev @types/node",
            "Line with Docker command: docker run -it --rm ubuntu:latest bash",
            "Line with Git command: git commit -m \"Initial commit\"",
            "Line with database connection: Server=localhost;Database=NPipeline;Trusted_Connection=true;",
            "Line with API endpoint: GET /api/v1/pipelines/{id}/status",
            "Line with log entry: [2023-12-25 14:30:45] INFO: Pipeline started successfully",
            "Line with error message: ERROR: Failed to connect to database after 3 attempts",
            "Line with warning: WARN: Configuration file not found, using defaults",
            "Line with debug info: DEBUG: Processing batch of 1000 records",
            "Final line of the sample file for demonstrating file processing capabilities",
        };

        await File.WriteAllLinesAsync(filePath, sampleLines);
        Console.WriteLine($"Created comprehensive sample input file with {sampleLines.Length} lines including various content types.");
    }
}
