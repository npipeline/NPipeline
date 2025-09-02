using System.Diagnostics;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     A fluent test harness for pipeline execution with assertion helpers.
/// </summary>
/// <typeparam name="TPipeline">The type of the pipeline definition to test.</typeparam>
/// <remarks>
///     <para>
///         PipelineTestHarness simplifies testing pipelines by providing:
///         - Fluent configuration methods
///         - Automatic error capturing for assertions
///         - Execution timing and metrics
///         - Direct access to pipeline context and results
///     </para>
///     <para>
///         Example usage:
///         <code>
/// var result = await new PipelineTestHarness&lt;MyPipeline&gt;()
///     .WithParameter("input", testData)
///     .CaptureErrors()
///     .RunAsync();
/// 
/// result.Success.Should().BeTrue();
/// result.Duration.Should().BeLessThan(TimeSpan.FromSeconds(1));
/// result.Errors.Should().BeEmpty();
/// </code>
///     </para>
/// </remarks>
public sealed class PipelineTestHarness<TPipeline> where TPipeline : IPipelineDefinition, new()
{
    private readonly List<Exception> _capturedErrors = new();
    private readonly IPipelineRunner _pipelineRunner;
    private bool _captureErrors;
    private PipelineErrorDecision _errorHandlingDecision = PipelineErrorDecision.ContinueWithoutNode;

    /// <summary>
    ///     Creates a new test harness for the specified pipeline definition.
    /// </summary>
    /// <param name="context">The pipeline context to use. Defaults to a new context if not provided.</param>
    /// <param name="pipelineRunner">The pipeline runner to use. Defaults to a new <see cref="PipelineRunner" /> if not provided.</param>
    public PipelineTestHarness(PipelineContext? context = null, IPipelineRunner? pipelineRunner = null)
    {
        Context = context ?? new PipelineContext();
        _pipelineRunner = pipelineRunner ?? new PipelineRunner();
    }

    /// <summary>
    ///     Gets the pipeline context being used by this harness.
    /// </summary>
    public PipelineContext Context { get; }

    /// <summary>
    ///     Gets the list of errors that have been captured so far.
    /// </summary>
    public IReadOnlyList<Exception> CapturedErrors => _capturedErrors.AsReadOnly();

    /// <summary>
    ///     Adds or updates a parameter in the pipeline context.
    /// </summary>
    /// <param name="key">The parameter key.</param>
    /// <param name="value">The parameter value.</param>
    /// <returns>This harness for fluent chaining.</returns>
    public PipelineTestHarness<TPipeline> WithParameter(string key, object value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        Context.Parameters[key] = value;
        return this;
    }

    /// <summary>
    ///     Adds multiple parameters to the pipeline context.
    /// </summary>
    /// <param name="parameters">A dictionary of parameters to add.</param>
    /// <returns>This harness for fluent chaining.</returns>
    public PipelineTestHarness<TPipeline> WithParameters(IDictionary<string, object> parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);

        foreach (var kvp in parameters)
        {
            Context.Parameters[kvp.Key] = kvp.Value;
        }

        return this;
    }

    /// <summary>
    ///     Adds an item to the pipeline context's Items dictionary.
    /// </summary>
    /// <param name="key">The item key.</param>
    /// <param name="value">The item value.</param>
    /// <returns>This harness for fluent chaining.</returns>
    public PipelineTestHarness<TPipeline> WithContextItem(string key, object value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        Context.Items[key] = value;
        return this;
    }

    /// <summary>
    ///     Sets the execution observer for the pipeline.
    /// </summary>
    /// <param name="observer">The observer to use.</param>
    /// <returns>This harness for fluent chaining.</returns>
    public PipelineTestHarness<TPipeline> WithExecutionObserver(IExecutionObserver observer)
    {
        Context.ExecutionObserver = observer;
        return this;
    }

    /// <summary>
    ///     Configures the harness to capture errors that occur during execution instead of throwing them.
    ///     Captured errors will be available in <see cref="PipelineExecutionResult.Errors" />.
    /// </summary>
    /// <param name="decision">The error decision to apply when errors are captured. Defaults to <see cref="PipelineErrorDecision.ContinueWithoutNode" />.</param>
    /// <returns>This harness for fluent chaining.</returns>
    public PipelineTestHarness<TPipeline> CaptureErrors(PipelineErrorDecision decision = PipelineErrorDecision.ContinueWithoutNode)
    {
        _captureErrors = true;
        _errorHandlingDecision = decision;
        return this;
    }

    /// <summary>
    ///     Runs the pipeline and returns the execution result.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to observe. If provided, overrides the context's token.</param>
    /// <returns>A result object containing success status, duration, errors, and the execution context.</returns>
    public async Task<PipelineExecutionResult> RunAsync(CancellationToken cancellationToken = default)
    {
        // Clear captured errors from previous runs to avoid polluting results
        _capturedErrors.Clear();

        var executionContext = Context;

        // If error capturing is enabled or a cancellation token was provided, create a new context
        if (_captureErrors || cancellationToken != default)
        {
            IPipelineErrorHandler? errorHandler;

            if (_captureErrors)
            {
                // Create the capturing handler
                CapturingPipelineErrorHandler capturingHandler = new(_capturedErrors, _errorHandlingDecision);

                // If there's an original handler, chain them together so both execute
                if (Context.PipelineErrorHandler is not null)
                    errorHandler = new CompositePipelineErrorHandler(Context.PipelineErrorHandler, capturingHandler);
                else
                {
                    // No original handler, just use the capturing handler
                    errorHandler = capturingHandler;
                }
            }
            else
            {
                // Not capturing, preserve the original handler
                errorHandler = Context.PipelineErrorHandler;
            }

            executionContext = new PipelineContext(
                new PipelineContextConfiguration(
                    CancellationToken: cancellationToken != default
                        ? cancellationToken
                        : Context.CancellationToken,
                    Parameters: Context.Parameters,
                    Items: Context.Items,
                    Properties: Context.Properties,
                    PipelineErrorHandler: errorHandler,
                    DeadLetterSink: Context.DeadLetterSink, // Preserve the dead-letter sink
                    ErrorHandlerFactory: Context.ErrorHandlerFactory,
                    LineageFactory: Context.LineageFactory,
                    ObservabilityFactory: Context.ObservabilityFactory,
                    LoggerFactory: Context.LoggerFactory,
                    Tracer: Context.Tracer,
                    RetryOptions: Context.RetryOptions));

            // Preserve the ExecutionObserver from the original context
            executionContext.ExecutionObserver = Context.ExecutionObserver;
        }

        var stopwatch = Stopwatch.StartNew();
        var success = true;
        List<Exception> uncaughtErrors = [];

        try
        {
            await _pipelineRunner.RunAsync<TPipeline>(executionContext);
        }
        catch (Exception ex)
        {
            success = false;
            uncaughtErrors.Add(ex);
        }
        finally
        {
            stopwatch.Stop();
        }

        // Combine any captured errors with uncaught exceptions
        List<Exception> allErrors =
        [
            .._capturedErrors,
            ..uncaughtErrors,
        ];

        return new PipelineExecutionResult(success, stopwatch.Elapsed, allErrors, executionContext);
    }
}
