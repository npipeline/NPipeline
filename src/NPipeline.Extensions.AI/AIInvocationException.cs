using NPipeline.ErrorHandling;

namespace NPipeline.Extensions.AI;

/// <summary>Base exception for failures reported by an AI provider integration.</summary>
public abstract class AIInvocationException : PipelineException
{
    /// <summary>Initializes an AI invocation exception.</summary>
    protected AIInvocationException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes an AI invocation exception.</summary>
    protected AIInvocationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Gets the provider identifier.</summary>
    public string? Provider { get; init; }

    /// <summary>Gets the model involved in the failed invocation.</summary>
    public string? Model { get; init; }

    /// <summary>Gets the provider request identifier.</summary>
    public string? RequestId { get; init; }
}
