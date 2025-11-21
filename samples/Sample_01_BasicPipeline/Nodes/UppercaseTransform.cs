using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_01_BasicPipeline.Nodes;

/// <summary>
///     Transform node that converts input strings to uppercase.
///     This node demonstrates how to create a simple transform that processes string data
///     by inheriting directly from TransformNode&lt;string, string&gt;.
/// </summary>
public class UppercaseTransform : TransformNode<string, string>
{
    /// <summary>
    ///     Converts the input string to uppercase.
    /// </summary>
    /// <param name="item">The input string to transform.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the uppercase string.</returns>
    public override async Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Transforming message: {item}");

        // Perform the uppercase transformation
        var transformedMessage = item.ToUpperInvariant();

        Console.WriteLine($"Transformed to: {transformedMessage}");

        return await Task.FromResult(transformedMessage);
    }
}
