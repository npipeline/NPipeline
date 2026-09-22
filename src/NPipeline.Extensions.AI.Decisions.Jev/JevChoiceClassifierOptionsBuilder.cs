using System.Text.Json.Nodes;

namespace NPipeline.Extensions.AI.Decisions.Jev;

/// <summary>Builds a typed Jev Choice classifier configuration.</summary>
public sealed class JevChoiceClassifierOptionsBuilder<TInput, TLabel>
    where TLabel : notnull
{
    private readonly List<JevChoiceDefinition<TLabel>> _choices = [];
    private JsonNode? _instructions;
    private string? _model;
    private string _questionId = "route";
    private Func<TInput, object?>? _stateFactory;

    /// <summary>Sets the state projected from each input.</summary>
    public JevChoiceClassifierOptionsBuilder<TInput, TLabel> WithState(Func<TInput, object?> stateFactory)
    {
        _stateFactory = stateFactory ?? throw new ArgumentNullException(nameof(stateFactory));
        return this;
    }

    /// <summary>Sets text instructions for the Choice question.</summary>
    public JevChoiceClassifierOptionsBuilder<TInput, TLabel> WithInstructions(string instructions)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instructions);
        _instructions = JsonValue.Create(instructions);
        return this;
    }

    /// <summary>Sets structured JSON instructions for the Choice question.</summary>
    public JevChoiceClassifierOptionsBuilder<TInput, TLabel> WithInstructions(JsonNode instructions)
    {
        _instructions = instructions ?? throw new ArgumentNullException(nameof(instructions));
        return this;
    }

    /// <summary>Adds a label and its wire representation.</summary>
    public JevChoiceClassifierOptionsBuilder<TInput, TLabel> AddChoice(
        TLabel label,
        string wireName,
        string? description = null) =>
        AddChoice(label, wireName, description is null
            ? null
            : JsonValue.Create(description));

    /// <summary>Adds a label with structured JSON criteria.</summary>
    public JevChoiceClassifierOptionsBuilder<TInput, TLabel> AddChoice(
        TLabel label,
        string wireName,
        JsonNode? description)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(wireName);
        _choices.Add(new JevChoiceDefinition<TLabel>(label, wireName, description));
        return this;
    }

    /// <summary>Overrides the model for this classifier.</summary>
    public JevChoiceClassifierOptionsBuilder<TInput, TLabel> WithModel(string model)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(model);
        _model = model;
        return this;
    }

    /// <summary>Overrides the question identifier used to correlate the answer.</summary>
    public JevChoiceClassifierOptionsBuilder<TInput, TLabel> WithQuestionId(string questionId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(questionId);
        _questionId = questionId;
        return this;
    }

    /// <summary>Builds and validates the configuration.</summary>
    public JevChoiceClassifierOptions<TInput, TLabel> Build()
    {
        if (_stateFactory is null)
            throw new InvalidOperationException("State is required.");

        if (_instructions is null)
            throw new InvalidOperationException("Instructions are required.");

        if (_choices.Count < 2)
            throw new InvalidOperationException("At least two choices are required.");

        if (_choices.Select(choice => choice.Label).Distinct().Count() != _choices.Count)
            throw new InvalidOperationException("Choice labels must be unique.");

        if (_choices.Select(choice => choice.WireName).Distinct(StringComparer.Ordinal).Count() != _choices.Count)
            throw new InvalidOperationException("Choice wire names must be unique.");

        if (_choices.Count > 255)
            throw new InvalidOperationException("A Choice question supports at most 255 options.");

        return new JevChoiceClassifierOptions<TInput, TLabel>
        {
            StateFactory = _stateFactory,
            Instructions = _instructions,
            Choices = _choices.ToArray(),
            QuestionId = _questionId,
            Model = _model,
        };
    }
}
