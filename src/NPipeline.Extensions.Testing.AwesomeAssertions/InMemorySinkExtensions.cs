using System.Linq.Expressions;
using AwesomeAssertions;

namespace NPipeline.Extensions.Testing.AwesomeAssertions;

/// <summary>
///     Contains extension methods for asserting on <see cref="InMemorySinkNode{T}" /> and <see cref="PipelineExecutionResult" />.
/// </summary>
public static class InMemorySinkExtensions
{
    /// <summary>
    ///     Asserts that the sinkNode has received a specific number of items.
    /// </summary>
    /// <param name="sinkNode">The sinkNode to assert on.</param>
    /// <param name="expectedCount">The expected number of items.</param>
    public static void ShouldHaveReceived<T>(this InMemorySinkNode<T> sinkNode, int expectedCount)
    {
        ArgumentNullException.ThrowIfNull(sinkNode);
        _ = sinkNode.Items.Count.Should().Be(expectedCount);
    }

    /// <summary>
    ///     Asserts that the sinkNode contains an item that matches a predicate.
    /// </summary>
    /// <param name="sinkNode">The sinkNode to assert on.</param>
    /// <param name="predicate">The predicate to match.</param>
    public static void ShouldContain<T>(this InMemorySinkNode<T> sinkNode, Expression<Func<T, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(sinkNode);
        ArgumentNullException.ThrowIfNull(predicate);
        _ = sinkNode.Items.Should().Contain(predicate);
    }

    /// <summary>
    ///     Asserts that the sinkNode contains the specified item.
    /// </summary>
    public static void ShouldContain<T>(this InMemorySinkNode<T> sinkNode, T expected)
    {
        ArgumentNullException.ThrowIfNull(sinkNode);
        _ = sinkNode.Items.Should().Contain(expected);
    }

    /// <summary>
    ///     Asserts that the sinkNode does not contain the specified item.
    /// </summary>
    public static void ShouldNotContain<T>(this InMemorySinkNode<T> sinkNode, T unexpected)
    {
        ArgumentNullException.ThrowIfNull(sinkNode);
        _ = sinkNode.Items.Should().NotContain(unexpected);
    }

    /// <summary>
    ///     Asserts that all items in the sinkNode satisfy the given predicate.
    /// </summary>
    public static void ShouldOnlyContain<T>(this InMemorySinkNode<T> sinkNode, Expression<Func<T, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(sinkNode);
        ArgumentNullException.ThrowIfNull(predicate);
        _ = sinkNode.Items.Should().OnlyContain(predicate);
    }

    /// <summary>
    ///     Asserts that the pipeline execution was successful (no uncaught exceptions).
    /// </summary>
    /// <param name="result">The execution result to assert on.</param>
    /// <returns>The same result for fluent chaining.</returns>
    public static PipelineExecutionResult ShouldBeSuccessful(this PipelineExecutionResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        _ = result.Success.Should().Be(true, "pipeline execution should succeed");
        return result;
    }

    /// <summary>
    ///     Asserts that the pipeline execution failed (has uncaught exceptions).
    /// </summary>
    /// <param name="result">The execution result to assert on.</param>
    /// <returns>The same result for fluent chaining.</returns>
    public static PipelineExecutionResult ShouldFail(this PipelineExecutionResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        _ = result.Success.Should().Be(false);
        return result;
    }

    /// <summary>
    ///     Asserts that no errors were captured during pipeline execution.
    /// </summary>
    /// <param name="result">The execution result to assert on.</param>
    /// <returns>The same result for fluent chaining.</returns>
    public static PipelineExecutionResult ShouldHaveNoErrors(this PipelineExecutionResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        _ = result.Errors.Should().BeEmpty();
        return result;
    }

    /// <summary>
    ///     Asserts that the specified number of errors were captured.
    /// </summary>
    /// <param name="result">The execution result to assert on.</param>
    /// <param name="expectedCount">The expected number of errors.</param>
    /// <returns>The same result for fluent chaining.</returns>
    public static PipelineExecutionResult ShouldHaveErrorCount(this PipelineExecutionResult result, int expectedCount)
    {
        ArgumentNullException.ThrowIfNull(result);
        _ = result.Errors.Count.Should().Be(expectedCount);
        return result;
    }

    /// <summary>
    ///     Asserts that at least one error of the specified type was captured.
    /// </summary>
    /// <typeparam name="TException">The expected exception type.</typeparam>
    /// <param name="result">The execution result to assert on.</param>
    /// <returns>The same result for fluent chaining.</returns>
    public static PipelineExecutionResult ShouldHaveErrorOfType<TException>(this PipelineExecutionResult result)
        where TException : Exception
    {
        ArgumentNullException.ThrowIfNull(result);
        _ = result.Errors.OfType<TException>().Should().NotBeEmpty();
        return result;
    }

    /// <summary>
    ///     Asserts that the execution completed within the specified time.
    /// </summary>
    /// <param name="result">The execution result to assert on.</param>
    /// <param name="maxDuration">The maximum allowed duration.</param>
    /// <returns>The same result for fluent chaining.</returns>
    public static PipelineExecutionResult ShouldCompleteWithin(this PipelineExecutionResult result, TimeSpan maxDuration)
    {
        ArgumentNullException.ThrowIfNull(result);
        _ = result.Duration.Should().BeLessThanOrEqualTo(maxDuration);
        return result;
    }
}
