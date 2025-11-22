using System.Text;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_FileProcessing.Nodes;

/// <summary>
///     Sink node for writing processed data to files.
///     This node demonstrates output to new files with proper resource management.
/// </summary>
public class FileSink : SinkNode<string>
{
    private readonly string? _outputFilePath;

    /// <summary>
    ///     Initializes a new instance of the <see cref="FileSink" /> class with a default output path.
    /// </summary>
    /// <param name="outputFilePath">The default path to the output file.</param>
    public FileSink(string outputFilePath)
    {
        _outputFilePath = outputFilePath ?? throw new ArgumentNullException(nameof(outputFilePath));
        Console.WriteLine($"Initializing FileSink for output: {outputFilePath}");
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="FileSink" /> class.
    ///     The output file path will be retrieved from pipeline context parameters.
    /// </summary>
    public FileSink()
    {
        Console.WriteLine("Initializing FileSink with parameterless constructor - output path will be retrieved from context");
    }

    /// <summary>
    ///     Executes the sink node asynchronously, writing processed lines to the output file.
    /// </summary>
    /// <param name="input">The input data pipe containing the lines to write.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Get output file path from context parameters if available, otherwise use the constructor parameter
        var outputFilePath = context.Parameters.TryGetValue("OutputFilePath", out var contextPath)
            ? contextPath.ToString()
            : _outputFilePath;

        if (string.IsNullOrEmpty(outputFilePath))
            throw new InvalidOperationException("FileSink: Output file path is not specified.");

        Console.WriteLine($"FileSink: Starting to write to file: {outputFilePath}");

        try
        {
            // Ensure the directory exists
            var directory = Path.GetDirectoryName(outputFilePath);

            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            {
                Directory.CreateDirectory(directory);
                Console.WriteLine($"FileSink: Created directory: {directory}");
            }

            // Use a temporary file for atomic write operation
            var tempFilePath = outputFilePath + ".tmp";
            var linesWritten = 0;

            // Write to temporary file first, then move to final destination
            using (var writer = new StreamWriter(tempFilePath, false, Encoding.UTF8))
            {
                await foreach (var line in input.WithCancellation(cancellationToken))
                {
                    await writer.WriteLineAsync(line.AsMemory(), cancellationToken);
                    linesWritten++;

                    // Log progress every 100 lines
                    if (linesWritten % 100 == 0)
                        Console.WriteLine($"FileSink: Written {linesWritten} lines...");
                }
            }

            // Atomically move the temporary file to the final destination
            File.Move(tempFilePath, outputFilePath, true);

            Console.WriteLine($"FileSink: Successfully wrote {linesWritten} lines to {outputFilePath}");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("FileSink: Operation was cancelled");
            throw;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"FileSink: Error writing to file: {ex.Message}");

            // Clean up temporary file if it exists
            var tempFilePath = outputFilePath + ".tmp";

            if (File.Exists(tempFilePath))
            {
                try
                {
                    File.Delete(tempFilePath);
                    Console.WriteLine($"FileSink: Cleaned up temporary file: {tempFilePath}");
                }
                catch (Exception cleanupEx)
                {
                    Console.WriteLine($"FileSink: Error cleaning up temporary file: {cleanupEx.Message}");
                }
            }

            throw;
        }
    }

    /// <summary>
    ///     Asynchronously disposes of the sink node resources.
    /// </summary>
    /// <returns>A ValueTask that represents the asynchronous dispose operation.</returns>
    public override ValueTask DisposeAsync()
    {
        Console.WriteLine("FileSink: Disposing resources...");
        GC.SuppressFinalize(this);
        return base.DisposeAsync();
    }
}
