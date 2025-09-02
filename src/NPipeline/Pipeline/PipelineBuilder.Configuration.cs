using System.Collections.Immutable;
using System.ComponentModel;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Lineage.Strategies;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
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

        if (!typeof(ITransformNode).IsAssignableFrom(nodeDef.NodeType))
            throw new InvalidOperationException(ErrorMessages.ExecutionStrategyCannotBeSetForNonTransformNode(nodeDef.Name, nodeDef.Kind.ToString()));

        NodeState.Nodes[handle.Id] = nodeDef with { ExecutionStrategy = strategy };
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

        var currentStrategy = nodeDef.ExecutionStrategy ?? new SequentialExecutionStrategy();

        if (currentStrategy is not ResilientExecutionStrategy)
            currentStrategy = new ResilientExecutionStrategy(currentStrategy);

        NodeState.Nodes[handle.Id] = nodeDef with { ExecutionStrategy = currentStrategy };
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

        NodeState.Nodes[handle.Id] = nodeDef with { ErrorHandlerType = errorHandlerType };
        return this;
    }

    public PipelineBuilder AddPipelineErrorHandler(IPipelineErrorHandler errorHandler)
    {
        ArgumentNullException.ThrowIfNull(errorHandler);
        ConfigurationState.PipelineErrorHandler = errorHandler;
        ConfigurationState.PipelineErrorHandlerType = null;
        return this;
    }

    public PipelineBuilder AddPipelineErrorHandler<T>() where T : IPipelineErrorHandler
    {
        ConfigurationState.PipelineErrorHandlerType = typeof(T);
        ConfigurationState.PipelineErrorHandler = null;
        return this;
    }

    public PipelineBuilder AddDeadLetterSink(IDeadLetterSink deadLetterSink)
    {
        ConfigurationState.DeadLetterSink = deadLetterSink;
        ConfigurationState.DeadLetterSinkType = null;
        return this;
    }

    public PipelineBuilder AddDeadLetterSink<T>() where T : IDeadLetterSink
    {
        ConfigurationState.DeadLetterSinkType = typeof(T);
        ConfigurationState.DeadLetterSink = null;
        return this;
    }

    public PipelineBuilder WithRetryOptions(Func<PipelineRetryOptions, PipelineRetryOptions> configure)
    {
        _config = _config with { RetryOptions = configure(_config.RetryOptions) };
        return this;
    }

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

    public PipelineBuilder ConfigureCircuitBreakerMemoryManagement(Func<CircuitBreakerMemoryManagementOptions, CircuitBreakerMemoryManagementOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var current = _config.CircuitBreakerMemoryOptions ?? CircuitBreakerMemoryManagementOptions.Default;
        _config = _config with { CircuitBreakerMemoryOptions = configure(current).Validate() };
        return this;
    }

    public PipelineBuilder EnableItemLevelLineage()
    {
        _config = _config with
        {
            ItemLevelLineageEnabled = true, LineageOptions = _config.LineageOptions ?? new LineageOptions { SampleEvery = 1, RedactData = false },
        };

        return this;
    }

    public PipelineBuilder EnableItemLevelLineage(Action<LineageOptions> configure)
    {
        var opts = _config.LineageOptions ?? new LineageOptions();
        configure(opts);
        _config = _config with { ItemLevelLineageEnabled = true, LineageOptions = opts };
        return this;
    }

    public PipelineBuilder AddLineageSink(ILineageSink lineageSink)
    {
        ConfigurationState.LineageSink = lineageSink;
        ConfigurationState.LineageSinkType = null;
        return this;
    }

    public PipelineBuilder AddLineageSink<T>() where T : ILineageSink
    {
        ConfigurationState.LineageSinkType = typeof(T);
        ConfigurationState.LineageSink = null;
        return this;
    }

    public PipelineBuilder AddPipelineLineageSink(IPipelineLineageSink pipelineLineageSink)
    {
        ConfigurationState.PipelineLineageSink = pipelineLineageSink;
        ConfigurationState.PipelineLineageSinkType = null;
        return this;
    }

    public PipelineBuilder AddPipelineLineageSink<T>() where T : IPipelineLineageSink
    {
        ConfigurationState.PipelineLineageSinkType = typeof(T);
        ConfigurationState.PipelineLineageSink = null;
        return this;
    }

    public PipelineBuilder WithRetryOptions(NodeHandle handle, PipelineRetryOptions options)
    {
        if (!NodeState.Nodes.ContainsKey(handle.Id))
            throw new InvalidOperationException(ErrorMessages.NodeNotFoundInBuilder(handle.Id, "WithRetryOptions"));

        NodeState.RetryOverrides[handle.Id] = options;
        return this;
    }

    public PipelineBuilder SetNodeExecutionOption(string nodeId, object option)
    {
        NodeState.ExecutionAnnotations[nodeId] = option;
        return this;
    }

    public PipelineBuilder SetGlobalExecutionObserver(object observer)
    {
        ConfigurationState.GlobalExecutionObserver = observer;
        return this;
    }

    public PipelineBuilder SetGlobalAnnotation(string key, object value)
    {
        NodeState.ExecutionAnnotations[$"global::{key}"] = value;
        return this;
    }

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
    ///     Enables extended validation, which runs additional checks beyond basic rules.
    /// </summary>
    /// <remarks>
    ///     Extended validation may be more expensive; use when you want comprehensive pipeline analysis.
    /// </remarks>
    public PipelineBuilder WithExtendedValidation()
    {
        _config = _config with { ExtendedValidation = true };
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

            // Mapper assumed stateless. Creation once; if it fails we'll fall back at runtime (throw). No caching of failure.
            cachedMapper = (ILineageMapper)Activator.CreateInstance(lineageMapperType)!;

        return (transformInput, nodeId, declaredCardinality, options, cancellationToken) =>
        {
            var typedInput = (IDataPipe<LineagePacket<TIn>>)transformInput;
            var unwrappedPipe = new StreamingDataPipe<TIn>(Unwrap(typedInput), $"Unwrapped_{typedInput.StreamName}");
            return (unwrappedPipe, RewrapFunc);

            IDataPipe RewrapFunc(IDataPipe outputPipe)
            {
                var typedOutputPipe = (IDataPipe<TOut>)outputPipe;
                var rewrappedStream = RewrapStrategy(typedInput, typedOutputPipe, nodeId, declaredCardinality, options, cancellationToken);
                return new StreamingDataPipe<LineagePacket<TOut>>(rewrappedStream, $"Rewrapped_{outputPipe.StreamName}");
            }

            static IAsyncEnumerable<TIn> Unwrap(IAsyncEnumerable<LineagePacket<TIn>> inputStream)
            {
                return UnwrapIterator(inputStream, CancellationToken.None);

                static async IAsyncEnumerable<TIn> UnwrapIterator(IAsyncEnumerable<LineagePacket<TIn>> inputStream,
                    [EnumeratorCancellation] CancellationToken ct)
                {
                    await foreach (var packet in inputStream.WithCancellation(ct).ConfigureAwait(false))
                    {
                        yield return packet.Data;
                    }
                }
            }

            IAsyncEnumerable<LineagePacket<TOut>> RewrapStrategy(
                IAsyncEnumerable<LineagePacket<TIn>> inputStream,
                IAsyncEnumerable<TOut> outputStream,
                string currentId,
                TransformCardinality transformCardinality,
                LineageOptions? lineageOptions,
                CancellationToken ct)
            {
                cachedStrategy ??= LineageMappingStrategySelector.Select<TIn, TOut>(lineageMapperType, transformCardinality, lineageOptions);

                // If we cached a mapper instance, pass its type (unchanged) – strategies currently create mappers internally when needed.
                // Future optimization: extend strategy interface to accept a mapper instance directly to avoid Activator calls.
                return cachedStrategy.MapAsync(inputStream, outputStream, currentId, transformCardinality, lineageOptions, lineageMapperType, cachedMapper, ct);
            }
        };
    }

    /// <summary>
    ///     Builds a lineage unwrap delegate for sink nodes.
    ///     This is an internal method used during node registration.
    /// </summary>
    private static SinkLineageUnwrapDelegate BuildSinkLineageUnwrapDelegate<TIn>()
    {
        return (lineageInput, lineageSink, sinkNodeId, options, ct) =>
        {
            var packetType = typeof(LineagePacket<>).MakeGenericType(typeof(TIn));
            var lineageInputType = lineageInput.GetType();
            IAsyncEnumerable<TIn> stream;

            if (lineageInput is IDataPipe<LineagePacket<TIn>> stronglyTyped)
                stream = Project(stronglyTyped, ct);
            else
            {
                var candidateInterface = lineageInputType.GetInterfaces().FirstOrDefault(i =>
                    i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDataPipe<>) && i.GetGenericArguments()[0].IsGenericType &&
                    i.GetGenericArguments()[0].GetGenericTypeDefinition() == typeof(LineagePacket<>));

                if (candidateInterface is not null)
                    stream = ProjectDynamic(lineageInput, ct);
                else
                {
                    throw new InvalidCastException(
                        $"Unable to treat sink lineage input as lineage stream for expected type '{typeof(TIn).Name}'. Actual: {lineageInputType.FullName}");
                }
            }

            return new StreamingDataPipe<TIn>(stream, $"Unwrapped_{lineageInput.StreamName}");

            async IAsyncEnumerable<TIn> Project(IDataPipe<LineagePacket<TIn>> input, [EnumeratorCancellation] CancellationToken token)
            {
                await foreach (var packet in input.WithCancellation(token).ConfigureAwait(false))
                {
                    if (lineageSink is not null && packet.Collect)
                    {
                        var finalPath = packet.TraversalPath.Add(sinkNodeId);

                        var dataToEmit = options?.RedactData == true
                            ? null
                            : (object?)packet.Data;

                        var hopRecords = (IReadOnlyList<LineageHop>)packet.LineageHops;
                        var lineageInfo = new LineageInfo(dataToEmit, packet.LineageId, finalPath, hopRecords);
                        await lineageSink.RecordAsync(lineageInfo, token).ConfigureAwait(false);
                    }

                    yield return packet.Data;
                }
            }

            async IAsyncEnumerable<TIn> ProjectDynamic(IDataPipe dynamicPipe, [EnumeratorCancellation] CancellationToken token)
            {
                PropertyInfo? dataProp = null, collectProp = null, lineageIdProp = null, pathProp = null, hopsProp = null;
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
                        lineageIdProp = objType.GetProperty("LineageId");
                        pathProp = objType.GetProperty("TraversalPath");
                        hopsProp = objType.GetProperty("LineageHops");
                        lastObservedType = objType;

                        if (dataProp is null || collectProp is null || lineageIdProp is null || pathProp is null || hopsProp is null)
                            continue; // malformed lineage packet type
                    }

                    var dataVal = dataProp!.GetValue(obj);

                    if (dataVal is TIn typedVal)
                    {
                        if (lineageSink is not null && (bool)collectProp!.GetValue(obj)!)
                        {
                            var finalPath = ((IImmutableList<string>)pathProp!.GetValue(obj)!).Add(sinkNodeId);
                            var hopRecords = (IReadOnlyList<LineageHop>)hopsProp!.GetValue(obj)!;

                            var dataToEmit = options?.RedactData == true
                                ? null
                                : (object?)typedVal;

                            var lineageInfo = new LineageInfo(dataToEmit, (Guid)lineageIdProp!.GetValue(obj)!, finalPath, hopRecords);
                            await lineageSink.RecordAsync(lineageInfo, token).ConfigureAwait(false);
                        }

                        yield return typedVal;
                    }
                }
            }
        };
    }

    #endregion
}
