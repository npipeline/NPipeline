using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     Represents the result of a pipeline execution in a test context.
/// </summary>
/// <param name="Success">Whether the pipeline executed successfully (no uncaught exceptions).</param>
/// <param name="Duration">The total time taken for pipeline execution.</param>
/// <param name="Errors">All exceptions that occurred during execution (may include caught errors if error handler captured them).</param>
/// <param name="Context">The pipeline context that was used for execution.</param>
/// <remarks>
///     This record is used by <see cref="PipelineTestHarness{TPipeline}" /> to provide comprehensive
///     execution results for test assertions and debugging.
/// </remarks>
public record PipelineExecutionResult(
    bool Success,
    TimeSpan Duration,
    IReadOnlyList<Exception> Errors,
    PipelineContext Context);
