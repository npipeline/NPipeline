using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_ObservabilityExtension.Nodes;

/// <summary>
///     Transform node that doubles numbers.
///     Demonstrates a simple transformation and recording metrics for each processed item.
/// </summary>
public class NumberMultiplier : TransformNode<int, int>
{
    /// <summary>
    ///     Doubles the input number.
    /// </summary>
    public override async Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
    {
        var result = item * 2;

        if (item <= 5 || item % 25 == 0) // Log a few examples
        {
            Console.WriteLine($"[NumberMultiplier] {item} × 2 = {result}");
        }

        return await Task.FromResult(result);
    }
}
