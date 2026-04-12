namespace NPipeline.ErrorHandling;

/// <summary>
///     Represents the full attribution chain for a node failure, capturing both
///     where the error originated and where the error handling decision was made.
/// </summary>
/// <param name="OriginNodeId">The node where the exception was originally thrown.</param>
/// <param name="DecisionNodeId">The node where the error handling decision (skip, dead-letter, fail) was made.</param>
/// <param name="OriginPipelineId">The pipeline containing the origin node.</param>
/// <param name="DecisionPipelineId">The pipeline containing the decision node.</param>
/// <param name="RunId">The run identifier, if available.</param>
/// <param name="CorrelationId">An item-level correlation identifier, if available.</param>
/// <param name="RetryCount">The number of retry attempts before this attribution was captured.</param>
public sealed record NodeFailureAttribution(
    string OriginNodeId,
    string DecisionNodeId,
    Guid OriginPipelineId,
    Guid DecisionPipelineId,
    Guid? RunId = null,
    Guid? CorrelationId = null,
    int RetryCount = 0);
