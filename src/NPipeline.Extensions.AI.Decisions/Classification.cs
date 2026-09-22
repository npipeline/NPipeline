namespace NPipeline.Extensions.AI.Decisions;

/// <summary>Classifies an input into one label and reports the complete probability distribution.</summary>
/// <typeparam name="TInput">The input type.</typeparam>
/// <typeparam name="TLabel">The label type.</typeparam>
public interface IAIClassifier<in TInput, TLabel>
    where TLabel : notnull
{
    /// <summary>Classifies one input.</summary>
    ValueTask<AIClassification<TLabel>> ClassifyAsync(
        TInput input,
        CancellationToken cancellationToken = default);
}

/// <summary>A typed classification with confidence, probabilities, and provider metadata.</summary>
/// <typeparam name="TLabel">The label type.</typeparam>
/// <param name="Label">The selected label.</param>
/// <param name="Confidence">The provider-reported confidence from zero to one.</param>
/// <param name="Probabilities">The probability distribution keyed by label.</param>
/// <param name="Metadata">Metadata for the invocation that produced the classification.</param>
public sealed record AIClassification<TLabel>(
    TLabel Label,
    double Confidence,
    IReadOnlyDictionary<TLabel, double> Probabilities,
    AIInvocationMetadata Metadata)
    where TLabel : notnull;

/// <summary>Associates an original pipeline item with its classification.</summary>
/// <typeparam name="TInput">The pipeline item type.</typeparam>
/// <typeparam name="TLabel">The label type.</typeparam>
/// <param name="Item">The original item.</param>
/// <param name="Classification">The classification produced for the item.</param>
public sealed record AIClassifiedItem<TInput, TLabel>(
    TInput Item,
    AIClassification<TLabel> Classification)
    where TLabel : notnull;
