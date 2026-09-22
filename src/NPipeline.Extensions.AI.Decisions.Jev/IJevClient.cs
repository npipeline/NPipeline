using System.Text.Json.Nodes;

namespace NPipeline.Extensions.AI.Decisions.Jev;

/// <summary>Evaluates state with TypeSafe System One models.</summary>
public interface IJevClient
{
    /// <summary>Gets the default model name.</summary>
    string DefaultModel { get; }

    /// <summary>Evaluates one state against one or more independent questions.</summary>
    ValueTask<JevSystemOneResponse> EvaluateAsync(
        JsonNode? state,
        IReadOnlyDictionary<string, JevQuestion> questions,
        string? model = null,
        CancellationToken cancellationToken = default);
}
