namespace NPipeline.Pipeline;

/// <summary>
///     Defines the contract for a pipeline runner that orchestrates pipeline execution.
/// </summary>
/// <remarks>
///     <para>
///         The pipeline runner is responsible for orchestrating the complete lifecycle of pipeline execution:
///         - Instantiating nodes from the pipeline definition
///         - Constructing the data flow graph
///         - Coordinating execution across all nodes
///         - Managing error handling and resilience
///         - Ensuring proper resource cleanup
///     </para>
///     <para>
///         The runner works with any pipeline definition implementing <see cref="IPipelineDefinition" />.
///         Each call to <see cref="RunAsync{TDefinition}" /> creates fresh instances of all nodes,
///         ensuring proper isolation between pipeline runs.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Define a pipeline
/// public class DataProcessingPipeline : IPipelineDefinition
/// {
///     public void Define(PipelineBuilder builder)
///     {
///         builder.AddSource&lt;CsvSourceNode&gt;("csv-reader");
///         builder.AddTransform&lt;DataValidationNode&gt;("validation");
///         builder.AddSink&lt;DatabaseSinkNode&gt;("db-writer");
///     }
/// }
/// 
/// // Run the pipeline
/// var runner = new PipelineRunner();
/// var context = PipelineContext.Default;
/// await runner.RunAsync&lt;DataProcessingPipeline&gt;(context);
/// </code>
/// </example>
public interface IPipelineRunner
{
    /// <summary>
    ///     Runs the pipeline defined by the specified definition.
    /// </summary>
    /// <typeparam name="TDefinition">The type of the pipeline definition.</typeparam>
    /// <param name="context">The pipeline context containing execution configuration, cancellation token, and services.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <remarks>
    ///     The runner will construct all nodes, connect them according to the pipeline definition,
    ///     and execute them. If any error occurs, the runner will invoke the error handler and
    ///     perform necessary cleanup. This method is fully asynchronous and respects the
    ///     cancellation token in the context.
    /// </remarks>
    Task RunAsync<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new();
}
