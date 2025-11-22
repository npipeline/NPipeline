using System.Reflection;
using Microsoft.Extensions.Hosting;
using NPipeline.Extensions.DependencyInjection;

namespace Sample_11_CsvConnector;

/// <summary>
///     Entry point for the CSV Connector sample demonstrating CSV data processing with NPipeline.
///     This sample shows how to read from CSV files, validate and transform data, and write to CSV files.
/// </summary>
public sealed class Program
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== NPipeline Sample: CSV Connector ===");
        Console.WriteLine();

        try
        {
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
            Console.WriteLine(CsvConnectorPipeline.GetDescription());
            Console.WriteLine();

            // Get the project directory to ensure paths work correctly from both IDE and command line
            var currentDir = Directory.GetCurrentDirectory();
            var projectDir = FindProjectDirectory(currentDir);

            // Set up pipeline parameters with absolute paths
            var pipelineParameters = new Dictionary<string, object>
            {
                ["SourcePath"] = Path.Combine(projectDir, "Data", "customers.csv"),
                ["TargetPath"] = Path.Combine(projectDir, "Data", "processed_customers.csv"),
            };

            Console.WriteLine("Pipeline Parameters:");
            Console.WriteLine($"  Source Path: {pipelineParameters["SourcePath"]}");
            Console.WriteLine($"  Target Path: {pipelineParameters["TargetPath"]}");
            Console.WriteLine();

            // Execute the pipeline using the DI container
            Console.WriteLine("Starting pipeline execution...");
            Console.WriteLine();

            await host.Services.RunPipelineAsync<CsvConnectorPipeline>(pipelineParameters);

            Console.WriteLine();
            Console.WriteLine("Pipeline execution completed successfully!");
            Console.WriteLine();

            // Display output file information
            var targetPath = (string)pipelineParameters["TargetPath"];

            if (File.Exists(targetPath))
            {
                var fileInfo = new FileInfo(targetPath);
                Console.WriteLine($"Output file created: {targetPath}");
                Console.WriteLine($"File size: {fileInfo.Length} bytes");
                Console.WriteLine($"Created: {fileInfo.CreationTime:yyyy-MM-dd HH:mm:ss}");

                // Display first few lines of the output file
                Console.WriteLine();
                Console.WriteLine("Sample output (first 5 lines):");
                var lines = File.ReadAllLines(targetPath);

                for (var i = 0; i < Math.Min(5, lines.Length); i++)
                {
                    Console.WriteLine($"  {lines[i]}");
                }

                if (lines.Length > 5)
                    Console.WriteLine($"  ... and {lines.Length - 5} more lines");
            }
            else
                Console.WriteLine($"Warning: Output file not found at {targetPath}");
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
}
