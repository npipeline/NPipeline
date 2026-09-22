using System.Text.Json.Nodes;
using System.Text.Json.Serialization;

namespace NPipeline.Extensions.AI.Decisions.Jev;

/// <summary>Base type for a TypeSafe System One question.</summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(JevChoiceQuestion), "choice")]
[JsonDerivedType(typeof(JevScoreQuestion), "score")]
[JsonDerivedType(typeof(JevNoulQuestion), "noul")]
public abstract record JevQuestion;

/// <summary>A question that selects one option from a fixed set.</summary>
public sealed record JevChoiceQuestion(
    JsonNode Instructions,
    IReadOnlyDictionary<string, JsonNode?> Criteria) : JevQuestion
{
    /// <summary>Initializes a Choice question from text instructions and criteria.</summary>
    public JevChoiceQuestion(string instructions, IReadOnlyDictionary<string, string?> criteria)
        : this(
            JsonValue.Create(instructions),
            criteria.ToDictionary(
                entry => entry.Key,
                entry => entry.Value is null
                    ? null
                    : (JsonNode?)JsonValue.Create(entry.Value),
                StringComparer.Ordinal))
    {
    }
}

/// <summary>A question that rates state against ordered levels.</summary>
public sealed record JevScoreQuestion(
    JsonNode Instructions,
    IReadOnlyList<JsonNode> Criteria) : JevQuestion
{
    /// <summary>Initializes a Score question from text instructions and levels.</summary>
    public JevScoreQuestion(string instructions, IReadOnlyList<string> criteria)
        : this(JsonValue.Create(instructions), criteria.Select(level => (JsonNode)JsonValue.Create(level)).ToArray())
    {
    }
}

/// <summary>A yes-or-no probability question.</summary>
public sealed record JevNoulQuestion(
    JsonNode Instructions,
    IReadOnlyDictionary<string, JsonNode?>? Criteria = null) : JevQuestion
{
    /// <summary>Initializes a Noul question from text instructions.</summary>
    public JevNoulQuestion(string instructions)
        : this(JsonValue.Create(instructions))
    {
    }
}

/// <summary>Base type for a TypeSafe System One answer.</summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "type")]
[JsonDerivedType(typeof(JevChoiceAnswer), "choice")]
[JsonDerivedType(typeof(JevScoreAnswer), "score")]
[JsonDerivedType(typeof(JevNoulAnswer), "noul")]
public abstract record JevAnswer;

/// <summary>A selected Choice option and its probability distribution.</summary>
public sealed record JevChoiceAnswer(
    string Choice,
    double Confidence,
    IReadOnlyDictionary<string, double> Probabilities) : JevAnswer;

/// <summary>A weighted Score and its level distribution.</summary>
public sealed record JevScoreAnswer(
    double Score,
    double Confidence,
    IReadOnlyDictionary<string, string> Legend,
    IReadOnlyDictionary<string, double> Probabilities) : JevAnswer;

/// <summary>The probability that a Noul question is true.</summary>
public sealed record JevNoulAnswer(double Noul) : JevAnswer;

/// <summary>Token usage for a System One request.</summary>
public sealed record JevUsage(
    [property: JsonPropertyName("input_tokens")]
    long InputTokens,
    [property: JsonPropertyName("output_tokens")]
    long OutputTokens);

/// <summary>A request to evaluate one state against one or more questions.</summary>
public sealed record JevSystemOneRequest(
    JsonNode? State,
    string Model,
    IReadOnlyDictionary<string, JevQuestion> Questions);

/// <summary>The answers and metadata returned by a System One model.</summary>
public sealed record JevSystemOneResponse(
    string Model,
    IReadOnlyDictionary<string, JevAnswer> Answers,
    JevUsage Usage)
{
    /// <summary>Gets the provider request identifier from the response headers.</summary>
    [JsonIgnore]
    public string? RequestId { get; init; }
}
