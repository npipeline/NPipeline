using NPipeline.Attributes;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Caching;
using NPipeline.Execution.Orchestration;
using NPipeline.Execution.Services;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Executes a pipeline defined by a <see cref="PipelineGraph" />.
///     <para>
///         The PipelineRunner is the main entry point for pipeline execution, coordinating all aspects
///         of pipeline processing including node instantiation, execution flow, error handling, and resource management.
///     </para>
///     <para>
///         Performance optimizations:
///         - Implements reflection-free steady state execution with pre-built execution plans
///         - Supports parallel sink execution when inputs are fully materialized
///     </para>
/// </summary>
/// <remarks>
///     This class is designed to be used with dependency injection or created via <see cref="Create" />.
/// </remarks>
public sealed class PipelineRunner(
    IPipelineFactory pipelineFactory,
    INodeFactory nodeFactory,
    INodeExecutor nodeExecutor,
    ITopologyService topologyService,
    INodeInstantiationService nodeInstantiationService,
    IErrorHandlingService errorHandlingService,
    IPersistenceService persistenceService,
    IObservabilitySurface observabilitySurface,
    ILineage lineage,
    IPipelineExecutionPlanCache? executionPlanCache = null,
    IRuntimePipelineBinder? runtimePipelineBinder = null) : IPipelineRunner
{
    private readonly IPipelineExecutionOrchestrator _executionOrchestrator = new PipelineExecutionOrchestrator(
        pipelineFactory,
        nodeFactory,
        nodeExecutor,
        topologyService,
        nodeInstantiationService,
        errorHandlingService,
        persistenceService,
        observabilitySurface,
        lineage,
        executionPlanCache ?? new InMemoryPipelineExecutionPlanCache(),
        runtimePipelineBinder ?? RuntimePipelineBinder.Instance);

    /// <inheritdoc />
    public async Task RunAsync<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new()
    {
        await RunAsync<TDefinition>(context, CancellationToken.None).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RunAsync(IPipelineDefinition definition, PipelineContext context, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(context);

        await RunAsyncCoreAsync(
                definition.GetType(),
                context,
                (factory, runtimeContext) => factory.Create(definition, runtimeContext),
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    ///     Creates a new PipelineRunner with default factories for simple use cases.
    ///     This method provides the most straightforward way to execute pipelines without
    ///     requiring explicit configuration of all dependencies.
    /// </summary>
    /// <example>
    ///     <code>
    ///     var runner = PipelineRunner.Create();
    ///     await runner.RunAsync&lt;MyPipelineDefinition&gt;();
    ///     </code>
    /// </example>
    public static PipelineRunner Create()
    {
        return new PipelineRunnerBuilder().Build();
    }

    /// <summary>
    ///     Runs a pipeline defined by <typeparamref name="TDefinition" /> using a default context and cancellation token.
    ///     This is the simplest way to execute a pipeline when no custom context is needed.
    /// </summary>
    /// <typeparam name="TDefinition">Pipeline definition type implementing <see cref="IPipelineDefinition" />.</typeparam>
    /// <param name="cancellationToken">Optional cancellation token for the pipeline execution.</param>
    /// <exception cref="PipelineExecutionException">Thrown when pipeline execution fails.</exception>
    /// <exception cref="NodeExecutionException">Thrown when a specific node fails execution.</exception>
    /// <exception cref="CircuitBreakerTrippedException">Thrown when the circuit breaker trips due to too many failures.</exception>
    /// <exception cref="RetryExhaustedException">Thrown when all retry attempts are exhausted.</exception>
    /// <example>
    ///     <code>
    ///     var runner = PipelineRunner.Create();
    ///     await runner.RunAsync&lt;MyDataProcessingPipeline&gt;(cancellationToken);
    ///     </code>
    /// </example>
    public async Task RunAsync<TDefinition>(CancellationToken cancellationToken = default) where TDefinition : IPipelineDefinition, new()
    {
        await using var context = new PipelineContext(
            PipelineContextConfiguration.WithCancellation(cancellationToken));

        await RunAsync<TDefinition>(context, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Runs a pipeline defined by <typeparamref name="TDefinition" /> using the provided <paramref name="context" /> and <paramref name="cancellationToken" />.
    ///     This method provides full control over the execution environment through the context parameter.
    /// </summary>
    /// <typeparam name="TDefinition">Pipeline definition type implementing <see cref="IPipelineDefinition" />.</typeparam>
    /// <param name="context">Execution context providing logging, tracing, factories, and parameters.</param>
    /// <param name="cancellationToken">Cancellation token for the pipeline execution.</param>
    /// <exception cref="PipelineExecutionException">Thrown when pipeline execution fails.</exception>
    /// <exception cref="NodeExecutionException">Thrown when a specific node fails execution.</exception>
    /// <exception cref="CircuitBreakerTrippedException">Thrown when the circuit breaker trips due to too many failures.</exception>
    /// <exception cref="RetryExhaustedException">Thrown when all retry attempts are exhausted.</exception>
    /// <example>
    ///     <code>
    ///     var context = PipelineContext.Default;
    ///     context.Items["customSetting"] = "value";
    ///     var runner = PipelineRunner.Create();
    ///     await runner.RunAsync&lt;MyDataProcessingPipeline&gt;(context, cancellationToken);
    ///     </code>
    /// </example>
    /// <remarks>
    ///     The execution process follows these steps:
    ///     1. Creates the pipeline instance from the definition
    ///     2. Instantiates all nodes in the graph
    ///     3. Builds execution plans for optimized performance
    ///     4. Executes nodes in topological order with error handling
    ///     5. Manages resource cleanup and persistence
    /// </remarks>
    public async Task RunAsync<TDefinition>(PipelineContext context, CancellationToken cancellationToken) where TDefinition : IPipelineDefinition, new()
    {
        ArgumentNullException.ThrowIfNull(context);

        await RunAsyncCoreAsync(
                typeof(TDefinition),
                context,
                static (pipelineFactory, runtimeContext) => pipelineFactory.Create<TDefinition>(runtimeContext),
                cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task RunAsyncCoreAsync(
        Type definitionType,
        PipelineContext context,
        Func<IPipelineFactory, PipelineContext, Pipeline.Pipeline> createPipeline,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitionType);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(createPipeline);

        if (string.IsNullOrWhiteSpace(context.PipelineName))
            context.PipelineName = PipelineAttributeHelper.GetPipelineName(definitionType);
        await _executionOrchestrator
            .RunAsync(definitionType, context, createPipeline, cancellationToken)
            .ConfigureAwait(false);
    }
}
