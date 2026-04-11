using System.Collections.Immutable;
using System.ComponentModel;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Lineage.Strategies;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Graph.PipelineDelegates;
using NPipeline.Graph.Validation;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Visualization;

namespace NPipeline.Pipeline;

/// <summary>
///     Configuration, validation, and lineage methods for PipelineBuilder.
///     Also includes internal lineage adapter construction.
/// </summary>
public sealed partial class PipelineBuilder
{
    /// <summary>
    ///     Method for setting execution strategies on nodes. Use fluent extension methods on node handles instead.
    /// </summary>
    /// <remarks>
    ///     This method is public to support fluent extensions in separate assemblies (e.g., parallelism extensions),
    ///     but is hidden from IntelliSense to discourage direct use. Always use the fluent extension methods on node handles.
    /// </remarks>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public PipelineBuilder WithExecutionStrategy(NodeHandle handle, IExecutionStrategy strategy)
    {
        ArgumentNullException.ThrowIfNull(handle);
        ArgumentNullException.ThrowIfNull(strategy);

        if (!NodeState.Nodes.TryGetValue(handle.Id, out var nodeDef))
            throw new InvalidOperationException(ErrorMessages.NodeNotFoundInBuilder(handle.Id, "WithExecutionStrategy"));

        if (!typeof(ITransformNode).IsAssignableFrom(nodeDef.NodeType) && !typeof(IStreamTransformNode).IsAssignableFrom(nodeDef.NodeType))
            throw new InvalidOperationException(ErrorMessages.ExecutionStrategyCannotBeSetForNonTransformNode(nodeDef.Name, nodeDef.Kind.ToString()));

        NodeState.Nodes[handle.Id] = nodeDef.WithExecutionStrategy(strategy);
        return this;
    }

    /// <summary>
    ///     Method for applying resilience wrapping to nodes. Use fluent extension methods on node handles instead.
    /// </summary>
    /// <remarks>
    ///     This method is public to support fluent extensions in separate assemblies,
    ///     but is hidden from IntelliSense to discourage direct use. Always use the fluent extension methods on node handles.
    /// </remarks>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public PipelineBuilder WithResilience(NodeHandle handle)
    {
        if (!NodeState.Nodes.TryGetValue(handle.Id, out var nodeDef))
            throw new InvalidOperationException(ErrorMessages.NodeNotFoundInBuilder(handle.Id, "WithResilience"));

        if (!typeof(ITransformNode).IsAssignableFrom(nodeDef.NodeType))
            throw new InvalidOperationException(ErrorMessages.ResilienceCannotBeAppliedToNonTransformNode(nodeDef.Name, nodeDef.Kind.ToString()));

        var currentStrategy = nodeDef.ExecutionStrategy ?? SequentialExecutionStrategy.Instance;

        if (currentStrategy is not ResilientExecutionStrategy)
            currentStrategy = new ResilientExecutionStrategy(currentStrategy);

        NodeState.Nodes[handle.Id] = nodeDef.WithExecutionStrategy(currentStrategy);
        return this;
    }

    /// <summary>
    ///     Method for setting error handlers on nodes. Use fluent extension methods on node handles instead.
    /// </summary>
    /// <remarks>
    ///     This method is public to support fluent extensions in separate assemblies,
    ///     but is hidden from IntelliSense to discourage direct use. Always use the fluent extension methods on node handles.
    /// </remarks>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public PipelineBuilder WithErrorHandler(NodeHandle handle, Type errorHandlerType)
    {
        ArgumentNullException.ThrowIfNull(handle);
        ArgumentNullException.ThrowIfNull(errorHandlerType);

        if (!NodeState.Nodes.TryGetValue(handle.Id, out var nodeDef))
            throw new InvalidOperationException(ErrorMessages.NodeNotFoundInBuilder(handle.Id, "WithErrorHandler"));

        if (!typeof(INodeErrorHandler).IsAssignableFrom(errorHandlerType))
            throw new ArgumentException(ErrorMessages.InvalidErrorHandlerType(errorHandlerType.Name), nameof(errorHandlerType));

        NodeState.Nodes[handle.Id] = nodeDef.WithErrorHandlerType(errorHandlerType);
        return this;
    }

    /// <summary>
    ///     Adds a pipeline error handler to handle errors that occur during pipeline execution.
    /// </summary>
    /// <param name="errorHandler">The error handler instance to use for pipeline-wide error handling.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddPipelineErrorHandler(IPipelineErrorHandler errorHandler)
    {
        ArgumentNullException.ThrowIfNull(errorHandler);
        ConfigurationState.PipelineErrorHandler = errorHandler;
        ConfigurationState.PipelineErrorHandlerType = null;
        return this;
    }

    /// <summary>
    ///     Adds a pipeline error handler of type T to handle errors that occur during pipeline execution.
    /// </summary>
    /// <typeparam name="T">The type of the error handler that implements IPipelineErrorHandler.</typeparam>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddPipelineErrorHandler<T>() where T : IPipelineErrorHandler
    {
        ConfigurationState.PipelineErrorHandlerType = typeof(T);
        ConfigurationState.PipelineErrorHandler = null;
        return this;
    }

    /// <summary>
    ///     Adds a dead letter sink to handle messages that cannot be processed after retry attempts.
    /// </summary>
    /// <param name="deadLetterSink">The dead letter sink instance to use for failed messages.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddDeadLetterSink(IDeadLetterSink deadLetterSink)
    {
        ConfigurationState.DeadLetterSink = deadLetterSink;
        ConfigurationState.DeadLetterSinkType = null;
        return this;
    }

    /// <summary>
    ///     Adds a dead letter sink of type T to handle messages that cannot be processed after retry attempts.
    /// </summary>
    /// <typeparam name="T">The type of the dead letter sink that implements IDeadLetterSink.</typeparam>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddDeadLetterSink<T>() where T : IDeadLetterSink
    {
        ConfigurationState.DeadLetterSinkType = typeof(T);
        ConfigurationState.DeadLetterSink = null;
        return this;
    }

    /// <summary>
    ///     Configures retry options for the pipeline using a configuration function.
    /// </summary>
    /// <param name="configure">A function that takes the current retry options and returns modified options.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder WithRetryOptions(Func<PipelineRetryOptions, PipelineRetryOptions> configure)
    {
        _config = _config with { RetryOptions = configure(_config.RetryOptions) };
        return this;
    }

    /// <summary>
    ///     Configures circuit breaker settings for the pipeline to handle failures gracefully.
    /// </summary>
    /// <param name="failureThreshold">The number of failures before opening the circuit breaker. Default is 5.</param>
    /// <param name="openDuration">The duration to keep the circuit breaker open. Default is 1 minute.</param>
    /// <param name="samplingWindow">The time window to sample for failure rate calculation. Default is 5 minutes.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder WithCircuitBreaker(int failureThreshold = 5, TimeSpan? openDuration = null, TimeSpan? samplingWindow = null)
    {
        _config = _config with
        {
            CircuitBreakerOptions =
            new PipelineCircuitBreakerOptions(failureThreshold, openDuration ?? TimeSpan.FromMinutes(1), samplingWindow ?? TimeSpan.FromMinutes(5))
                .Validate(),
        };

        return this;
    }

    /// <summary>
    ///     Configures memory management options for the circuit breaker.
    /// </summary>
    /// <param name="configure">A function that takes the current memory management options and returns modified options.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder ConfigureCircuitBreakerMemoryManagement(Func<CircuitBreakerMemoryManagementOptions, CircuitBreakerMemoryManagementOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var current = _config.CircuitBreakerMemoryOptions ?? CircuitBreakerMemoryManagementOptions.Default;
        _config = _config with { CircuitBreakerMemoryOptions = configure(current).Validate() };
        return this;
    }

    /// <summary>
    ///     Enables item-level lineage tracking with default options.
    /// </summary>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder EnableItemLevelLineage()
    {
        _config = _config with
        {
            ItemLevelLineageEnabled = true,
            LineageOptions = _config.LineageOptions ?? new LineageOptions(SampleEvery: 1, RedactData: false),
        };

        return this;
    }

    /// <summary>
    ///     Enables item-level lineage tracking with custom configuration options.
    /// </summary>
    /// <param name="configure">An action to configure the lineage options.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder EnableItemLevelLineage(Action<LineageOptions> configure)
    {
        var opts = _config.LineageOptions ?? LineageOptions.Default;
        configure(opts);
        _config = _config with { ItemLevelLineageEnabled = true, LineageOptions = opts };
        return this;
    }

    /// <summary>
    ///     Adds a lineage sink to record item-level lineage information.
    /// </summary>
    /// <param name="lineageSink">The lineage sink instance to use for recording lineage data.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddLineageSink(ILineageSink lineageSink)
    {
        ConfigurationState.LineageSink = lineageSink;
        ConfigurationState.LineageSinkType = null;
        return this;
    }

    /// <summary>
    ///     Adds a lineage sink of type T to record item-level lineage information.
    /// </summary>
    /// <typeparam name="T">The type of the lineage sink that implements ILineageSink.</typeparam>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddLineageSink<T>() where T : ILineageSink
    {
        ConfigurationState.LineageSinkType = typeof(T);
        ConfigurationState.LineageSink = null;
        return this;
    }

    /// <summary>
    ///     Adds a pipeline lineage sink to record pipeline-level lineage information.
    /// </summary>
    /// <param name="pipelineLineageSink">The pipeline lineage sink instance to use for recording pipeline lineage data.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddPipelineLineageSink(IPipelineLineageSink pipelineLineageSink)
    {
        ConfigurationState.PipelineLineageSink = pipelineLineageSink;
        ConfigurationState.PipelineLineageSinkType = null;
        return this;
    }

    /// <summary>
    ///     Adds a pipeline lineage sink of type T to record pipeline-level lineage information.
    /// </summary>
    /// <typeparam name="T">The type of the pipeline lineage sink that implements IPipelineLineageSink.</typeparam>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddPipelineLineageSink<T>() where T : IPipelineLineageSink
    {
        ConfigurationState.PipelineLineageSinkType = typeof(T);
        ConfigurationState.PipelineLineageSink = null;
        return this;
    }

    /// <summary>
    ///     Sets retry options for a specific node identified by its handle.
    /// </summary>
    /// <param name="handle">The handle of the node to configure retry options for.</param>
    /// <param name="options">The retry options to apply to the node.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder WithRetryOptions(NodeHandle handle, PipelineRetryOptions options)
    {
        if (!NodeState.Nodes.ContainsKey(handle.Id))
            throw new InvalidOperationException(ErrorMessages.NodeNotFoundInBuilder(handle.Id, "WithRetryOptions"));

        NodeState.RetryOverrides[handle.Id] = options;
        return this;
    }

    /// <summary>
    ///     Sets an execution option for a specific node identified by its ID.
    /// </summary>
    /// <param name="nodeId">The ID of the node to set the execution option for.</param>
    /// <param name="option">The execution option to set for the node.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder SetNodeExecutionOption(string nodeId, object option)
    {
        NodeState.ExecutionAnnotations[nodeId] = option;
        return this;
    }

    /// <summary>
    ///     Sets a global execution observer that will monitor execution across all nodes.
    /// </summary>
    /// <param name="observer">The observer instance to use for global execution monitoring.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder SetGlobalExecutionObserver(object observer)
    {
        ConfigurationState.GlobalExecutionObserver = observer;
        return this;
    }

    /// <summary>
    ///     Sets a global annotation with the specified key and value.
    /// </summary>
    /// <param name="key">The key for the global annotation.</param>
    /// <param name="value">The value for the global annotation.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder SetGlobalAnnotation(string key, object value)
    {
        NodeState.ExecutionAnnotations[$"global::{key}"] = value;
        return this;
    }

    /// <summary>
    ///     Adds a visualizer to generate visual representations of the pipeline.
    /// </summary>
    /// <param name="visualizer">The visualizer instance to use for pipeline visualization.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddVisualizer(IPipelineVisualizer visualizer)
    {
        ConfigurationState.Visualizer = visualizer;
        return this;
    }

    #region Validation Configuration

    /// <summary>
    ///     Sets the validation mode for the pipeline graph.
    /// </summary>
    /// <remarks>
    ///     Determines whether validation issues cause exceptions (Error), warnings (Warn), or are ignored (Off).
    /// </remarks>
    public PipelineBuilder WithValidationMode(GraphValidationMode mode)
    {
        _config = _config with { GraphValidationMode = mode };
        return this;
    }

    /// <summary>
    ///     Adds a custom validation rule to the pipeline graph.
    /// </summary>
    /// <remarks>
    ///     Custom rules are evaluated when building the pipeline unless validation is disabled.
    /// </remarks>
    public PipelineBuilder WithValidationRule(IGraphRule rule)
    {
        _customValidationRules.Add(rule);
        return this;
    }

    /// <summary>
    ///     Disables extended validation rules (enabled by default).
    /// </summary>
    /// <remarks>
    ///     Extended validation includes additional checks for best practices (resilience configuration,
    ///     parallel execution settings, etc.). It's enabled by default for safety. Disable only if you
    ///     need maximum build performance and are confident in your configuration.
    /// </remarks>
    public PipelineBuilder WithoutExtendedValidation()
    {
        _config = _config with { ExtendedValidation = false };
        return this;
    }

    /// <summary>
    ///     Enables early name validation to catch duplicate node names as they're added.
    /// </summary>
    /// <remarks>
    ///     By default, name validation occurs at build time. This option validates names immediately.
    /// </remarks>
    public PipelineBuilder WithEarlyNameValidation()
    {
        _config = _config with { EarlyNameValidation = true };
        return this;
    }

    /// <summary>
    ///     Disables early name validation, allowing duplicate names to be caught at build time instead.
    /// </summary>
    /// <remarks>
    ///     Early name validation is enabled by default. Use this method to disable it and rely on build-time validation instead.
    /// </remarks>
    public PipelineBuilder WithoutEarlyNameValidation()
    {
        _config = _config with { EarlyNameValidation = false };
        return this;
    }

    #endregion

    #region Internal Lineage Construction

    /// <summary>
    ///     Builds a lineage adapter delegate for transform nodes.
    ///     This is an internal method used during node registration.
    /// </summary>
    private static LineageAdapterDelegate BuildLineageAdapter<TIn, TOut>(Type? lineageMapperType)
    {
        // Cache strategy & mapper instance (if any) per adapter instance to avoid repeated reflection / selection.
        ILineageMappingStrategy<TIn, TOut>? cachedStrategy = null;
        ILineageMapper? cachedMapper = null;

        if (lineageMapperType is not null)
        {
            // Mapper assumed stateless. Creation once; if it fails we'll fall back at runtime (throw). No caching of failure.
            cachedMapper = (ILineageMapper)Activator.CreateInstance(lineageMapperType)!;
        }

        return (transformInput, nodeId, pipelineId, pipelineName, declaredCardinality, options, cancellationToken) =>
        {
            var typedInput = (IDataStream<LineagePacket<TIn>>)transformInput;
            LineageNodeOutcomeRegistry.BeginNode(pipelineId, nodeId);

            // Single-enumeration: read typedInput exactly once via a background pump that
            // fans out into two channels — one carrying unwrapped TIn items for the transform,
            // one carrying full LineagePacket<TIn> for the rewrap strategy. This prevents
            // multiple GetAsyncEnumerator() calls on upstream multicast streams and eliminates
            // the cascading re-enumeration issue through passthrough wrappers.
            var dataChannel = Channel.CreateUnbounded<(long Index, Guid CorrelationId, int[]? AncestryInputIndices, TIn Data)>(
                new UnboundedChannelOptions { SingleWriter = true });

            var packetChannel = Channel.CreateUnbounded<LineagePacket<TIn>>(new UnboundedChannelOptions { SingleWriter = true });

            _ = PumpInputAsync(typedInput, dataChannel.Writer, packetChannel.Writer, cancellationToken);

            var unwrappedPipe = new DataStream<TIn>(ProjectWithInputIndex(dataChannel.Reader.ReadAllAsync(cancellationToken), cancellationToken),
                $"Unwrapped_{typedInput.StreamName}");

            return (unwrappedPipe, RewrapFunc);

            IDataStream RewrapFunc(IDataStream outputPipe)
            {
                var typedOutputPipe = (IDataStream<TOut>)outputPipe;

                var rewrappedStream = RewrapStrategy(packetChannel.Reader.ReadAllAsync(cancellationToken),
                    typedOutputPipe, nodeId, pipelineId, pipelineName, declaredCardinality, options, cancellationToken);

                var cleanupStream = CleanupOnComplete(rewrappedStream, pipelineId, nodeId, cancellationToken);
                return new DataStream<LineagePacket<TOut>>(cleanupStream, $"Rewrapped_{outputPipe.StreamName}");
            }

            IAsyncEnumerable<LineagePacket<TOut>> RewrapStrategy(
                IAsyncEnumerable<LineagePacket<TIn>> inputStream,
                IAsyncEnumerable<TOut> outputStream,
                string currentId,
                Guid currentPipelineId,
                string? currentPipelineName,
                TransformCardinality transformCardinality,
                LineageOptions? lineageOptions,
                CancellationToken ct)
            {
                cachedStrategy ??= SelectLineageMappingStrategy<TIn, TOut>(lineageMapperType, transformCardinality, lineageOptions);

                return cachedStrategy.MapAsync(inputStream, outputStream, currentId, currentPipelineId, currentPipelineName, transformCardinality,
                    lineageOptions, lineageMapperType, cachedMapper, ct);
            }

            static async IAsyncEnumerable<TIn> ProjectWithInputIndex(
                IAsyncEnumerable<(long Index, Guid CorrelationId, int[]? AncestryInputIndices, TIn Data)> source,
                [EnumeratorCancellation] CancellationToken ct)
            {
                try
                {
                    await foreach (var (index, correlationId, ancestryInputIndices, data) in source.WithCancellation(ct).ConfigureAwait(false))
                    {
                        LineageExecutionItemContext.SetCurrentInputContext(index, correlationId, ancestryInputIndices);
                        yield return data;
                    }
                }
                finally
                {
                    LineageExecutionItemContext.ClearCurrentInputIndex();
                }
            }

            static async IAsyncEnumerable<LineagePacket<TOut>> CleanupOnComplete(
                IAsyncEnumerable<LineagePacket<TOut>> source,
                Guid currentPipelineId,
                string currentNodeId,
                [EnumeratorCancellation] CancellationToken ct = default)
            {
                try
                {
                    await foreach (var packet in source.WithCancellation(ct).ConfigureAwait(false))
                    {
                        yield return packet;
                    }
                }
                finally
                {
                    LineageNodeOutcomeRegistry.ClearNode(currentPipelineId, currentNodeId);
                }
            }
        };

        static async Task PumpInputAsync(
            IDataStream<LineagePacket<TIn>> source,
            ChannelWriter<(long Index, Guid CorrelationId, int[]? AncestryInputIndices, TIn Data)> dataWriter,
            ChannelWriter<LineagePacket<TIn>> packetWriter,
            CancellationToken ct)
        {
            try
            {
                long inputIndex = 0;

                await foreach (var packet in source.WithCancellation(ct).ConfigureAwait(false))
                {
                    int[]? ancestryInputIndices = null;

                    if (packet.LineageHops.Count > 0)
                    {
                        var latestHop = packet.LineageHops[^1];

                        if (latestHop.AncestryInputIndices is { Count: > 0 })
                            ancestryInputIndices = [.. latestHop.AncestryInputIndices];
                    }

                    // Write packet first so that the strategy's inputStream has data available
                    // before the transform's outputStream is triggered by consuming dataChannel.
                    await packetWriter.WriteAsync(packet, ct).ConfigureAwait(false);
                    await dataWriter.WriteAsync((inputIndex, packet.CorrelationId, ancestryInputIndices, packet.Data), ct).ConfigureAwait(false);
                    inputIndex++;
                }

                packetWriter.TryComplete();
                dataWriter.TryComplete();
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                packetWriter.TryComplete();
                dataWriter.TryComplete();
            }
            catch (Exception ex)
            {
                packetWriter.TryComplete(ex);
                dataWriter.TryComplete(ex);
            }
        }
    }

    /// <summary>
    ///     Builds a lineage unwrap delegate for sink nodes.
    ///     This is an internal method used during node registration.
    /// </summary>
    private static SinkLineageUnwrapDelegate BuildSinkLineageUnwrapDelegate<TIn>()
    {
        return (lineageInput, lineageSink, sinkNodeId, pipelineId, pipelineName, options, ct) =>
        {
            var packetType = typeof(LineagePacket<>).MakeGenericType(typeof(TIn));
            var lineageInputType = lineageInput.GetType();
            IAsyncEnumerable<TIn> stream;

            if (lineageInput is IDataStream<LineagePacket<TIn>> stronglyTyped)
                stream = Project(stronglyTyped, ct);
            else
            {
                var candidateInterface = lineageInputType.GetInterfaces().FirstOrDefault(i =>
                    i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDataStream<>) && i.GetGenericArguments()[0].IsGenericType &&
                    i.GetGenericArguments()[0].GetGenericTypeDefinition() == typeof(LineagePacket<>));

                if (candidateInterface is not null)
                    stream = ProjectDynamic(lineageInput, ct);
                else
                {
                    throw new InvalidCastException(
                        $"Unable to treat sink lineage input as lineage stream for expected type '{typeof(TIn).Name}'. Actual: {lineageInputType.FullName}");
                }
            }

            return new DataStream<TIn>(stream, $"Unwrapped_{lineageInput.StreamName}");

            async IAsyncEnumerable<TIn> Project(IDataStream<LineagePacket<TIn>> input, [EnumeratorCancellation] CancellationToken token)
            {
                await foreach (var packet in input.WithCancellation(token).ConfigureAwait(false))
                {
                    if (lineageSink is not null && packet.Collect)
                    {
                        var finalPath = packet.TraversalPath.Add($"{pipelineId:N}::{sinkNodeId}");

                        var dataToEmit = options?.RedactData == true
                            ? null
                            : (object?)packet.Data;

                        var hopRecords = (IReadOnlyList<LineageHop>)packet.LineageHops;
                        var lineageInfo = new LineageInfo(dataToEmit, packet.CorrelationId, finalPath, hopRecords, pipelineId, pipelineName);
                        await lineageSink.RecordAsync(lineageInfo, token).ConfigureAwait(false);
                    }

                    yield return packet.Data;
                }
            }

            async IAsyncEnumerable<TIn> ProjectDynamic(IDataStream dynamicPipe, [EnumeratorCancellation] CancellationToken token)
            {
                PropertyInfo? dataProp = null, collectProp = null, correlationIdProp = null, pathProp = null, hopsProp = null;
                Type? lastObservedType = null;

                await foreach (var obj in dynamicPipe.ToAsyncEnumerable(token).WithCancellation(token).ConfigureAwait(false))
                {
                    if (obj is null)
                        continue;

                    var objType = obj.GetType();

                    if (!objType.IsGenericType || objType.GetGenericTypeDefinition() != typeof(LineagePacket<>))
                        continue;

                    if (!ReferenceEquals(objType, lastObservedType))
                    {
                        dataProp = objType.GetProperty("Data");
                        collectProp = objType.GetProperty("Collect");
                        correlationIdProp = objType.GetProperty("CorrelationId");
                        pathProp = objType.GetProperty("TraversalPath");
                        hopsProp = objType.GetProperty("LineageHops");
                        lastObservedType = objType;

                        if (dataProp is null || collectProp is null || correlationIdProp is null || pathProp is null || hopsProp is null)
                            continue; // malformed lineage packet type
                    }

                    var dataVal = dataProp!.GetValue(obj);

                    if (dataVal is TIn typedVal)
                    {
                        if (lineageSink is not null && (bool)collectProp!.GetValue(obj)!)
                        {
                            var finalPath = ((IImmutableList<string>)pathProp!.GetValue(obj)!).Add($"{pipelineId:N}::{sinkNodeId}");
                            var hopRecords = (IReadOnlyList<LineageHop>)hopsProp!.GetValue(obj)!;

                            var dataToEmit = options?.RedactData == true
                                ? null
                                : (object?)typedVal;

                            var lineageInfo = new LineageInfo(dataToEmit, (Guid)correlationIdProp!.GetValue(obj)!, finalPath, hopRecords, pipelineId,
                                pipelineName);

                            await lineageSink.RecordAsync(lineageInfo, token).ConfigureAwait(false);
                        }

                        yield return typedVal;
                    }
                }
            }
        };
    }

    /// <summary>
    ///     Selects the appropriate lineage mapping strategy based on mapper type, cardinality, and options.
    /// </summary>
    private static ILineageMappingStrategy<TIn, TOut> SelectLineageMappingStrategy<TIn, TOut>(
        Type? mapperType, TransformCardinality cardinality, LineageOptions? options)
    {
        if (mapperType is null && cardinality == TransformCardinality.OneToOne)
            return StreamingOneToOneStrategy<TIn, TOut>.Instance;

        var cap = options?.MaterializationCap;

        if (cap is not null && cap > 0)
            return CapAwareMaterializingStrategy<TIn, TOut>.Instance;

        return MaterializingStrategy<TIn, TOut>.Instance;
    }

    #endregion
}
