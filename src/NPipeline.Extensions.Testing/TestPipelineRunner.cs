using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     A helper class for running pipelines in testing scenarios and retrieving results.
/// </summary>
public sealed class TestPipelineRunner
{
    private readonly IPipelineRunner _pipelineRunner;

    /// <summary>
    ///     Initializes a new instance of the <see cref="TestPipelineRunner" /> class.
    /// </summary>
    /// <param name="pipelineRunner">The pipeline runner to use for executing the pipeline.</param>
    public TestPipelineRunner(IPipelineRunner pipelineRunner)
    {
        _pipelineRunner = pipelineRunner;
    }

    /// <summary>
    ///     Runs a pipeline and returns the result from an <see cref="InMemorySinkNode{T}" />.
    /// </summary>
    /// <typeparam name="TDefinition">The type of the pipeline definition.</typeparam>
    /// <typeparam name="TResult">The type of the result expected in the sink.</typeparam>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The items collected by the <see cref="InMemorySinkNode{T}" />.</returns>
    public async Task<IReadOnlyList<TResult>> RunAndGetResultAsync<TDefinition, TResult>(PipelineContext context) where TDefinition : IPipelineDefinition, new()
    {
        try
        {
            await _pipelineRunner.RunAsync<TDefinition>(context);
        }
        catch (OperationCanceledException oce)
        {
            // Some tests (particularly in the testing project) expect a PipelineExecutionException
            // with an inner OperationCanceledException. Wrap here to preserve that test contract
            // without changing the runner behavior used by observability/integration tests.
            throw new PipelineExecutionException("Pipeline execution was canceled.", oce);
        }

        if (!context.Items.TryGetValue(typeof(InMemorySinkNode<TResult>).FullName!, out var sinkObject) || sinkObject is not InMemorySinkNode<TResult> sink)
        {
            throw new InvalidOperationException(
                $"Could not find an instance of '{typeof(InMemorySinkNode<TResult>).Name}' in the pipeline context. Make sure it is registered as the sink in your pipeline definition.");
        }

        return await sink.Completion;
    }

    /// <summary>
    ///     Retrieves a sink instance stored by the pipeline under PipelineContext.Items.
    ///     This is a convenience wrapper over context.GetSink{T}().
    /// </summary>
    public T GetSink<T>(PipelineContext context) where T : class
    {
        ArgumentNullException.ThrowIfNull(context);
        return context.GetSink<T>();
    }
}
