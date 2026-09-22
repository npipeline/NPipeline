using System.Collections.Frozen;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace NPipeline.Extensions.AI.Decisions.Jev;

/// <summary>Classifies inputs using a TypeSafe Choice question.</summary>
public sealed class JevChoiceClassifier<TInput, TLabel> : IAIClassifier<TInput, TLabel>
    where TLabel : notnull
{
    private readonly FrozenDictionary<TLabel, JevChoiceDefinition<TLabel>> _choicesByLabel;
    private readonly FrozenDictionary<string, JevChoiceDefinition<TLabel>> _choicesByWireName;
    private readonly IJevClient _client;
    private readonly string? _model;
    private readonly FrozenDictionary<string, JevQuestion> _questions;
    private readonly Func<TInput, object?> _stateFactory;

    /// <summary>Initializes a classifier from validated options.</summary>
    public JevChoiceClassifier(IJevClient client, JevChoiceClassifierOptions<TInput, TLabel> options)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        ArgumentNullException.ThrowIfNull(options);

        _stateFactory = options.StateFactory;
        _model = options.Model;
        _choicesByLabel = options.Choices.ToFrozenDictionary(choice => choice.Label);
        _choicesByWireName = options.Choices.ToFrozenDictionary(choice => choice.WireName, StringComparer.Ordinal);
        var criteria = options.Choices.ToFrozenDictionary(choice => choice.WireName, choice => choice.Description, StringComparer.Ordinal);

        _questions = new Dictionary<string, JevQuestion>(1, StringComparer.Ordinal)
        {
            [options.QuestionId] = new JevChoiceQuestion(options.Instructions, criteria),
        }.ToFrozenDictionary(StringComparer.Ordinal);

        QuestionId = options.QuestionId;
    }

    private string QuestionId { get; }

    /// <inheritdoc />
    public async ValueTask<AIClassification<TLabel>> ClassifyAsync(TInput input, CancellationToken cancellationToken = default)
    {
        var stateValue = _stateFactory(input);

        var state = stateValue switch
        {
            null => null,
            JsonNode node => node,
            _ => JsonSerializer.SerializeToNode(stateValue, stateValue.GetType()),
        };

        var response = await _client.EvaluateAsync(state, _questions, _model, cancellationToken).ConfigureAwait(false);

        if (!response.Answers.TryGetValue(QuestionId, out var answer))
            throw new InvalidOperationException($"The TypeSafe response did not contain the '{QuestionId}' answer.");

        if (answer is not JevChoiceAnswer choiceAnswer)
            throw new InvalidOperationException($"The TypeSafe answer '{QuestionId}' was '{answer.GetType().Name}', not a Choice answer.");

        if (!_choicesByWireName.TryGetValue(choiceAnswer.Choice, out var selected))
            throw new InvalidOperationException($"The TypeSafe response selected unknown choice '{choiceAnswer.Choice}'.");

        var probabilities = new Dictionary<TLabel, double>(_choicesByLabel.Count);

        foreach (var choice in _choicesByLabel.Values)
        {
            if (!choiceAnswer.Probabilities.TryGetValue(choice.WireName, out var probability))
                throw new InvalidOperationException($"The TypeSafe response omitted probability '{choice.WireName}'.");

            probabilities.Add(choice.Label, probability);
        }

        return new AIClassification<TLabel>(
            selected.Label,
            choiceAnswer.Confidence,
            probabilities.ToFrozenDictionary(),
            new AIInvocationMetadata(
                "typesafe",
                response.Model,
                response.RequestId,
                new AIUsage(response.Usage.InputTokens, response.Usage.OutputTokens)));
    }
}

/// <summary>Validated configuration for a Jev Choice classifier.</summary>
public sealed record JevChoiceClassifierOptions<TInput, TLabel>
    where TLabel : notnull
{
    /// <summary>Gets the state projection.</summary>
    public required Func<TInput, object?> StateFactory { get; init; }

    /// <summary>Gets the Choice instructions.</summary>
    public required JsonNode Instructions { get; init; }

    /// <summary>Gets the configured choices.</summary>
    public required IReadOnlyList<JevChoiceDefinition<TLabel>> Choices { get; init; }

    /// <summary>Gets the answer correlation identifier.</summary>
    public required string QuestionId { get; init; }

    /// <summary>Gets the optional model override.</summary>
    public string? Model { get; init; }
}

/// <summary>Maps a typed label to a TypeSafe Choice option.</summary>
public sealed record JevChoiceDefinition<TLabel>(TLabel Label, string WireName, JsonNode? Description)
    where TLabel : notnull;
