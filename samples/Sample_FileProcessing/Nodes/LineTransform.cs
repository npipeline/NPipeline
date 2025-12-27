using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_FileProcessing.Nodes;

/// <summary>
///     Transform node for processing text lines from files.
///     This node demonstrates line-by-line transformation of file content.
/// </summary>
public class LineTransform : TransformNode<string, string>
{
    private readonly bool _addLineNumbers;
    private readonly bool _convertToUpperCase;
    private readonly string _prefix;
    private int _lineNumber;

    /// <summary>
    ///     Initializes a new instance of the <see cref="LineTransform" /> class.
    /// </summary>
    /// <param name="prefix">The prefix to add to each line.</param>
    /// <param name="addLineNumbers">Whether to add line numbers to each line.</param>
    /// <param name="convertToUpperCase">Whether to convert lines to uppercase.</param>
    public LineTransform(string prefix = "PROCESSED: ", bool addLineNumbers = true, bool convertToUpperCase = false)
    {
        _prefix = prefix ?? throw new ArgumentNullException(nameof(prefix));
        _addLineNumbers = addLineNumbers;
        _convertToUpperCase = convertToUpperCase;
        _lineNumber = 0;

        Console.WriteLine($"Initializing LineTransform node with prefix='{prefix}', addLineNumbers={addLineNumbers}, convertToUpperCase={convertToUpperCase}");
    }

    /// <summary>
    ///     Executes the transform node asynchronously, processing individual lines from the file.
    /// </summary>
    /// <param name="item">The input line to transform.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation, returning the transformed line.</returns>
    public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        try
        {
            // Increment line number if tracking is enabled
            if (_addLineNumbers)
                _lineNumber++;

            Console.WriteLine($"LineTransform: Processing line: {item[..Math.Min(50, item.Length)]}...");

            // Apply transformations
            var transformedLine = TransformLine(item);

            Console.WriteLine($"LineTransform: Transformed to: {transformedLine[..Math.Min(50, transformedLine.Length)]}...");

            return Task.FromResult(transformedLine);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"LineTransform: Error transforming line: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    ///     Transforms a single line of text according to the configured options.
    /// </summary>
    /// <param name="inputLine">The input line to transform.</param>
    /// <returns>The transformed line.</returns>
    private string TransformLine(string inputLine)
    {
        var result = inputLine;

        // Convert to uppercase if requested
        if (_convertToUpperCase)
            result = result.ToUpperInvariant();

        // Add line number if requested
        if (_addLineNumbers)
            result = $"[Line {_lineNumber:D4}] {result}";

        // Add prefix
        result = $"{_prefix}{result}";

        return result;
    }
}
