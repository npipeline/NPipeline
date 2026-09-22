using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Decisions.Jev;

/// <summary>Extension methods for adding Jev-powered decision routes.</summary>
public static class PipelineBuilderExtensions
{
    /// <summary>Adds a Jev Choice classifier followed by a provider-neutral decision route.</summary>
    public static AIRouteBuilder<TInput, TLabel> AddJevRoute<TInput, TLabel>(
        this PipelineBuilder builder,
        IJevClient client,
        Action<JevChoiceClassifierOptionsBuilder<TInput, TLabel>> configure,
        string? name = null)
        where TLabel : notnull
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new JevChoiceClassifierOptionsBuilder<TInput, TLabel>();
        configure(optionsBuilder);
        var classifier = new JevChoiceClassifier<TInput, TLabel>(client, optionsBuilder.Build());
        return builder.AddAIRoute(classifier, name);
    }
}
