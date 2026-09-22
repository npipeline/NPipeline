using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Decisions;

/// <summary>Classifies each input while preserving the original item.</summary>
public sealed class AIClassificationNode<TInput, TLabel> : TransformNode<TInput, AIClassifiedItem<TInput, TLabel>>
    where TLabel : notnull
{
    private readonly IAIClassifier<TInput, TLabel> _classifier;

    /// <summary>Initializes the node with a classifier.</summary>
    public AIClassificationNode(IAIClassifier<TInput, TLabel> classifier)
    {
        _classifier = classifier ?? throw new ArgumentNullException(nameof(classifier));
    }

    /// <inheritdoc />
    public override async ValueTask<AIClassifiedItem<TInput, TLabel>> TransformAsync(
        TInput item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var classification = await _classifier.ClassifyAsync(item, cancellationToken).ConfigureAwait(false);
        return new AIClassifiedItem<TInput, TLabel>(item, classification);
    }
}
