using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_FileProcessing.Nodes;

/// <summary>
///     Source node for reading text files line by line using streaming.
///     This node demonstrates file-based source nodes with proper resource management.
/// </summary>
public class TextFileSource : SourceNode<string>
{
    /// <summary>
    ///     Executes the source node asynchronously, reading the text file line by line.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An <see cref="IDataPipe{TOut}" /> that produces the output data for downstream nodes.</returns>
    public override IDataPipe<string> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        // Get file path from context parameters
        if (!context.Parameters.TryGetValue("FilePath", out var filePathObj))
            throw new InvalidOperationException("FilePath parameter is required but not found in pipeline context.");

        var filePath = filePathObj.ToString() ?? throw new InvalidOperationException("FilePath parameter cannot be null.");

        Console.WriteLine($"TextFileSource: Starting to read file: {filePath}");

        try
        {
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"Input file not found: {filePath}");

            Console.WriteLine($"TextFileSource: Reading file: {filePath}");

            // Create a streaming async enumerable that reads the file line by line
            var lineStream = ReadLinesAsync(filePath, cancellationToken);

            // Return a streaming data pipe that will process lines as they are requested
            return new StreamingDataPipe<string>(lineStream, "TextFileSource");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"TextFileSource: Error reading file: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    ///     Reads lines from the file asynchronously using streaming.
    /// </summary>
    /// <param name="filePath">The path to the file to read.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An async enumerable of strings representing the lines in the file.</returns>
    private async IAsyncEnumerable<string> ReadLinesAsync(
        string filePath,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(filePath);
        string? line;
        var lineNumber = 0;

        while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
        {
            lineNumber++;

            var displayText = line.Length > 50
                ? line[..50] + "..."
                : line;

            Console.WriteLine($"TextFileSource: Read line {lineNumber}: {displayText}");
            yield return line;
        }

        Console.WriteLine($"TextFileSource: Finished reading {lineNumber} lines from {filePath}");
    }
}
