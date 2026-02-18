using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_GcsStorageProvider.Nodes;

/// <summary>
///     Transform node that processes text documents.
///     This node demonstrates how to create a simple transform that processes
///     text data by inheriting from TransformNode&lt;string, string&gt;.
/// </summary>
public class TextTransform : TransformNode<string, string>
{
    /// <summary>
    ///     Transforms the input text by converting to uppercase and adding a prefix.
    /// </summary>
    /// <param name="item">The input text to transform.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the transformed text.</returns>
    public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Transforming document ({item.Length} chars)");

        // Perform a simple transformation: uppercase and add metadata
        var transformed = $"[PROCESSED at {DateTime.UtcNow:O}]\n{item.ToUpperInvariant()}";

        Console.WriteLine($"  Transformed to {transformed.Length} chars");

        return Task.FromResult(transformed);
    }
}
