namespace NPipeline.Graph.Validation;

/// <summary>
///     Exception thrown when pipeline validation fails.
/// </summary>
public sealed class PipelineValidationException : Exception
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="PipelineValidationException" /> class with a default message.
    /// </summary>
    public PipelineValidationException() : base("Pipeline validation failed.")
    {
        Result = PipelineValidationResult.Success;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="PipelineValidationException" /> class with a specified message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public PipelineValidationException(string message) : base($"Pipeline validation failed: {message}")
    {
        Result = PipelineValidationResult.Success;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="PipelineValidationException" /> class with a specified error message and a reference to the inner exception
    ///     that is the cause of this exception.
    /// </summary>
    /// <param name="message">The error message that explains the reason for the exception.</param>
    /// <param name="inner">The exception that is the cause of the current exception.</param>
    public PipelineValidationException(string message, Exception inner) : base($"Pipeline validation failed: {message}", inner)
    {
        Result = PipelineValidationResult.Success;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="PipelineValidationException" /> class with a validation result containing the validation errors.
    /// </summary>
    /// <param name="result">The validation result containing the issues that caused the exception.</param>
    public PipelineValidationException(PipelineValidationResult result)
        : base($"Pipeline validation failed: {string.Join("; ", result.Errors)}")
    {
        Result = result;
    }

    /// <summary>
    ///     Gets the validation result containing all issues that caused the exception.
    /// </summary>
    public PipelineValidationResult Result { get; }
}
