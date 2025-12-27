using NPipeline.Execution;
using NPipeline.Execution.Caching;
using NPipeline.Execution.Factories;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Common;

/// <summary>
///     Helpers for configuring caching behavior in unit tests.
/// </summary>
public static class TestCachingHelpers
{
    /// <summary>
    ///     Creates a PipelineRunner with execution plan caching disabled.
    /// </summary>
    /// <remarks>
    ///     Use this when tests require guaranteed fresh compilation:
    ///     - Testing stateful nodes where state must be isolated between runs
    ///     - Measuring compilation overhead (caching would skew benchmarks)
    ///     - Verifying expression tree structure changes are detected
    ///     Most tests don't need this since different pipeline types/structures
    ///     result in automatic cache misses. Only use when you specifically need
    ///     to guarantee fresh compilation for each test run.
    /// </remarks>
    /// <param name="pipelineFactory">Optional custom pipeline factory. Defaults to <see cref="PipelineFactory" />.</param>
    /// <param name="nodeFactory">Optional custom node factory. Defaults to <see cref="DefaultNodeFactory" />.</param>
    /// <returns>A PipelineRunner with caching disabled.</returns>
    public static IPipelineRunner CreateRunnerWithoutCaching(
        IPipelineFactory? pipelineFactory = null,
        INodeFactory? nodeFactory = null)
    {
        return new PipelineRunnerBuilder()
            .WithPipelineFactory(pipelineFactory ?? new PipelineFactory())
            .WithNodeFactory(nodeFactory ?? new DefaultNodeFactory())
            .WithoutExecutionPlanCache()
            .Build();
    }

    /// <summary>
    ///     Creates a PipelineRunner with a specific execution plan cache implementation.
    /// </summary>
    /// <remarks>
    ///     Use this to test custom cache implementations or to use alternative caching strategies
    ///     like Redis or distributed caches in integration tests.
    /// </remarks>
    /// <param name="cache">The cache implementation to use.</param>
    /// <param name="pipelineFactory">Optional custom pipeline factory. Defaults to <see cref="PipelineFactory" />.</param>
    /// <param name="nodeFactory">Optional custom node factory. Defaults to <see cref="DefaultNodeFactory" />.</param>
    /// <returns>A PipelineRunner with the specified cache implementation.</returns>
    public static IPipelineRunner CreateRunnerWithCache(
        IPipelineExecutionPlanCache cache,
        IPipelineFactory? pipelineFactory = null,
        INodeFactory? nodeFactory = null)
    {
        return new PipelineRunnerBuilder()
            .WithPipelineFactory(pipelineFactory ?? new PipelineFactory())
            .WithNodeFactory(nodeFactory ?? new DefaultNodeFactory())
            .WithExecutionPlanCache(cache)
            .Build();
    }

    /// <summary>
    ///     Creates a PipelineRunner with default settings (caching enabled).
    /// </summary>
    /// <remarks>
    ///     This is equivalent to using PipelineRunnerBuilder directly with default settings.
    ///     Provided for consistency with other TestCachingHelpers methods.
    /// </remarks>
    /// <param name="pipelineFactory">Optional custom pipeline factory. Defaults to <see cref="PipelineFactory" />.</param>
    /// <param name="nodeFactory">Optional custom node factory. Defaults to <see cref="DefaultNodeFactory" />.</param>
    /// <returns>A PipelineRunner with caching enabled (default behavior).</returns>
    public static IPipelineRunner CreateRunnerWithCaching(
        IPipelineFactory? pipelineFactory = null,
        INodeFactory? nodeFactory = null)
    {
        return new PipelineRunnerBuilder()
            .WithPipelineFactory(pipelineFactory ?? new PipelineFactory())
            .WithNodeFactory(nodeFactory ?? new DefaultNodeFactory())
            .Build();
    }
}
