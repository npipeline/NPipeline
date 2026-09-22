using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Decisions;

/// <summary>Extension methods for adding provider-neutral AI decision routes.</summary>
public static class PipelineBuilderExtensions
{
    /// <summary>Adds a classifier followed by a confidence-aware route node.</summary>
    public static AIRouteBuilder<TInput, TLabel> AddAIRoute<TInput, TLabel>(
        this PipelineBuilder builder,
        IAIClassifier<TInput, TLabel> classifier,
        string? name = null)
        where TLabel : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(classifier);

        var baseName = name ?? $"AI_{typeof(TLabel).Name}_Route";

        var classificationHandle = builder.AddTransform<AIClassificationNode<TInput, TLabel>, TInput, AIClassifiedItem<TInput, TLabel>>(
            $"{baseName}_classify");

        builder.AddPreconfiguredNodeInstance(classificationHandle.Id, new AIClassificationNode<TInput, TLabel>(classifier));

        var routeHandle = builder.AddRoute<AIClassifiedItem<TInput, TLabel>>($"{baseName}_route");
        builder.Connect(classificationHandle, routeHandle);

        return new AIRouteBuilder<TInput, TLabel>(builder, classificationHandle, routeHandle, baseName);
    }
}
