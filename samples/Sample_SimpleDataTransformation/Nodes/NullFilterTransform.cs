using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_SimpleDataTransformation.Nodes;

/// <summary>
///     Transform node that filters out placeholder items (those with ID 0).
///     This node demonstrates how to create a transform that filters data
///     by inheriting directly from TransformNode&lt;Person, Person&gt;.
/// </summary>
public class NullFilterTransform : TransformNode<Person, Person>
{
    /// <summary>
    ///     Processes the input Person object and returns it if it's valid (ID != 0),
    ///     otherwise returns a placeholder Person with ID 0.
    /// </summary>
    /// <param name="item">The Person object to filter.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the filter operation with a Person result.</returns>
    public override Task<Person> ExecuteAsync(Person item, PipelineContext context, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Filter out placeholder items (those with ID 0)
        if (item.Id == 0)
        {
            // Return a placeholder Person with ID 0 to indicate filtered item
            // This will be handled by the next transform in the pipeline
            return Task.FromResult(new Person(0, string.Empty, string.Empty, 0, string.Empty, string.Empty));
        }

        // Return the original item if it's valid
        return Task.FromResult(item);
    }
}
