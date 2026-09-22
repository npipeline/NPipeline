namespace NPipeline.Extensions.AI;

/// <summary>Describes the provider invocation that produced an AI result.</summary>
/// <param name="Provider">The provider identifier.</param>
/// <param name="Model">The model that produced the result.</param>
/// <param name="RequestId">The provider request identifier.</param>
/// <param name="Usage">The token usage reported by the provider.</param>
public sealed record AIInvocationMetadata(
    string Provider,
    string? Model = null,
    string? RequestId = null,
    AIUsage? Usage = null);

/// <summary>Token usage reported for an AI invocation.</summary>
/// <param name="InputTokens">The number of input tokens.</param>
/// <param name="OutputTokens">The number of output tokens.</param>
public sealed record AIUsage(long? InputTokens = null, long? OutputTokens = null);
