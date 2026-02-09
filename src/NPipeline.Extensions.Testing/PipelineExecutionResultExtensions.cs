namespace NPipeline.Extensions.Testing;

/// <summary>
///     Extension methods for asserting on <see cref="PipelineExecutionResult" />.
/// </summary>
/// <remarks>
///     These helpers provide convenient methods for common test assertions on pipeline execution results.
///     They integrate well with assertion libraries like AwesomeAssertions.
/// </remarks>
public static class PipelineExecutionResultExtensions
{
    /// <summary>
    ///     Asserts that the pipeline execution was successful (no uncaught exceptions).
    /// </summary>
    /// <param name="result">The execution result to assert on.</param>
    /// <returns>The same result for further assertions or context access.</returns>
    /// <exception cref="InvalidOperationException">Thrown if execution was not successful.</exception>
    public static PipelineExecutionResult AssertSuccess(this PipelineExecutionResult result)
    {
        if (!result.Success)
        {
            var errorMessage = result.Errors.Any()
                ? $"Pipeline execution failed with {result.Errors.Count} error(s): {string.Join(", ", result.Errors.Select(e => e.Message))}"
                : "Pipeline execution failed with an unknown error";

            throw new InvalidOperationException(errorMessage);
        }

        return result;
    }

    /// <summary>
    ///     Asserts that the pipeline execution failed (has uncaught exceptions).
    /// </summary>
    /// <param name="result">The execution result to assert on.</param>
    /// <returns>The same result for further assertions or context access.</returns>
    /// <exception cref="InvalidOperationException">Thrown if execution was successful.</exception>
    public static PipelineExecutionResult AssertFailure(this PipelineExecutionResult result)
    {
        if (result.Success)
            throw new InvalidOperationException("Expected pipeline execution to fail, but it succeeded.");

        return result;
    }

    /// <summary>
    ///     Asserts that no errors were captured during pipeline execution.
    /// </summary>
    /// <param name="result">The execution result to assert on.</param>
    /// <returns>The same result for further assertions or context access.</returns>
    /// <exception cref="InvalidOperationException">Thrown if errors were captured.</exception>
    public static PipelineExecutionResult AssertNoErrors(this PipelineExecutionResult result)
    {
        if (result.Errors.Any())
        {
            var errorMessage = $"Expected no errors, but found {result.Errors.Count}: {string.Join(", ", result.Errors.Select(e => e.Message))}";
            throw new InvalidOperationException(errorMessage);
        }

        return result;
    }

    /// <summary>
    ///     Asserts that at least one error of the specified type was captured.
    /// </summary>
    /// <typeparam name="TException">The expected exception type.</typeparam>
    /// <param name="result">The execution result to assert on.</param>
    /// <returns>The same result for further assertions or context access.</returns>
    /// <exception cref="InvalidOperationException">Thrown if no error of the specified type was found.</exception>
    public static PipelineExecutionResult AssertErrorOfType<TException>(this PipelineExecutionResult result)
        where TException : Exception
    {
        if (!result.Errors.OfType<TException>().Any())
        {
            var errorMessage =
                $"Expected at least one error of type {typeof(TException).Name}, but none were found. Errors: {string.Join(", ", result.Errors.Select(e => $"{e.GetType().Name}: {e.Message}"))}";

            throw new InvalidOperationException(errorMessage);
        }

        return result;
    }

    /// <summary>
    ///     Asserts that the specified number of errors were captured.
    /// </summary>
    /// <param name="result">The execution result to assert on.</param>
    /// <param name="expectedCount">The expected number of errors.</param>
    /// <returns>The same result for further assertions or context access.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the error count doesn't match.</exception>
    public static PipelineExecutionResult AssertErrorCount(this PipelineExecutionResult result, int expectedCount)
    {
        if (result.Errors.Count != expectedCount)
        {
            var errorMessage =
                $"Expected {expectedCount} error(s), but found {result.Errors.Count}. Errors: {string.Join(", ", result.Errors.Select(e => e.Message))}";

            throw new InvalidOperationException(errorMessage);
        }

        return result;
    }

    /// <summary>
    ///     Asserts that the execution completed within the specified time.
    /// </summary>
    /// <param name="result">The execution result to assert on.</param>
    /// <param name="maxDuration">The maximum allowed duration.</param>
    /// <returns>The same result for further assertions or context access.</returns>
    /// <exception cref="InvalidOperationException">Thrown if execution took longer than expected.</exception>
    public static PipelineExecutionResult AssertCompletedWithin(this PipelineExecutionResult result, TimeSpan maxDuration)
    {
        if (result.Duration > maxDuration)
        {
            throw new InvalidOperationException(
                $"Expected pipeline to complete within {maxDuration.TotalMilliseconds}ms, but it took {result.Duration.TotalMilliseconds}ms");
        }

        return result;
    }

    /// <summary>
    ///     Gets the sink of the specified type from the execution context.
    /// </summary>
    /// <typeparam name="T">The type of sink to retrieve.</typeparam>
    /// <param name="result">The execution result.</param>
    /// <returns>The sink instance from the context.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the sink type is not found in the context.</exception>
    public static T GetSink<T>(this PipelineExecutionResult result) where T : class
    {
        return result.Context.GetSink<T>();
    }

    /// <summary>
    ///     Tries to get a value from the context's Items dictionary.
    /// </summary>
    /// <typeparam name="T">The expected type of the value.</typeparam>
    /// <param name="result">The execution result.</param>
    /// <param name="key">The key to look up.</param>
    /// <param name="value">The retrieved value, or null if not found.</param>
    /// <returns>True if the key was found and the value is of the expected type; otherwise false.</returns>
    public static bool TryGetContextItem<T>(this PipelineExecutionResult result, string key, out T? value) where T : class
    {
        if (result.Context.Items.TryGetValue(key, out var obj) && obj is T typedValue)
        {
            value = typedValue;
            return true;
        }

        value = null;
        return false;
    }
}
