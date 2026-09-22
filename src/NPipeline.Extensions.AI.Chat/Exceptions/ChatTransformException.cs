namespace NPipeline.Extensions.AI.Chat.Exceptions;

/// <summary>Exception thrown when a chat transform or enrichment fails during pipeline execution.</summary>
public sealed class ChatTransformException : AIInvocationException
{
    /// <summary>Initializes a new instance with the specified error message and inner exception.</summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The exception that caused the failure.</param>
    public ChatTransformException(string message, Exception innerException)
        : base(message, innerException)
    {
        ErrorCode = "CHAT_TRANSFORM_ERROR";
        Provider = "chat";
    }

    /// <summary>The error code for this exception type.</summary>
    public string ErrorCode { get; }

    /// <summary>The item being processed when the failure occurred.</summary>
    public object? OriginalItem { get; init; }

    /// <summary>The prompt that was sent to the LLM.</summary>
    public string? PromptSent { get; init; }

    /// <summary>The model that was in use when the failure occurred.</summary>
    public string? ModelUsed { get; init; }

    /// <summary>The raw response text from the LLM, if available.</summary>
    public string? RawResponse { get; init; }
}
