using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution.Lineage.Strategies;
using NPipeline.Lineage;

namespace NPipeline.Execution.Services;

/// <summary>
///     Provides services for managing item-level lineage throughout a pipeline execution.
/// </summary>
public sealed class LineageService : ILineageService
{
    private const int SmallMaterializationThreshold = 256;
    private static readonly ConcurrentDictionary<Type, Func<IDataStream, string, Guid, string?, LineageOptions?, IDataStream>> WrapSourceDelegates = new();

    private static readonly ConcurrentDictionary<Type, Func<object, string, Guid, string?, LineageOptions?, HopDecisionFlags, CancellationToken, object>>
        WrapJoinOutputsDelegates = new();

    private static readonly ConcurrentDictionary<Type, Func<object, IAsyncEnumerable<object?>, string, Guid, string?, LineageOptions?,
        HopDecisionFlags, Type?, CancellationToken, object>> WrapNodeOutputsFromInputDelegates = new();

    private static readonly ConcurrentDictionary<Type, Func<IEnumerable, object>> EnumerableToAsyncDelegates = new();
    private static readonly ConcurrentDictionary<Type, ILineageMapper> MapperInstances = new();

    /// <inheritdoc />
    public IDataStream WrapSourceStream(IDataStream sourcePipe, string nodeId, Guid pipelineId, string? pipelineName, LineageOptions? options)
    {
        var dataType = sourcePipe.GetDataType();
        var del = WrapSourceDelegates.GetOrAdd(dataType, static t => BuildWrapSourceDelegate(t));
        return del(sourcePipe, nodeId, pipelineId, pipelineName, options);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<object> UnwrapLineageStream(IAsyncEnumerable<object?> source, [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
        {
            if (item is null)
            {
                yield return null!;

                continue;
            }

            yield return UnwrapIfLineage(item);
        }
    }

    /// <inheritdoc />
    public (IDataStream unwrappedInput, IAsyncEnumerable<object?> inputLineageContext) PrepareInputWithLineageContext(
        IDataStream source,
        CancellationToken ct = default)
    {
        var dataChannel = Channel.CreateUnbounded<object?>(new UnboundedChannelOptions { SingleWriter = true });
        var contextChannel = Channel.CreateUnbounded<object?>(new UnboundedChannelOptions { SingleWriter = true });

        _ = PumpInputWithLineageContextAsync(source.ToAsyncEnumerable(ct), dataChannel.Writer, contextChannel.Writer, ct);

        IDataStream unwrapped = new DataStream<object?>(dataChannel.Reader.ReadAllAsync(ct), $"Unwrapped_{source.StreamName}");
        return (unwrapped, contextChannel.Reader.ReadAllAsync(ct));
    }

    /// <inheritdoc />
    public IDataStream WrapNodeOutput(IDataStream output, string currentNodeId, Guid pipelineId, string? pipelineName, LineageOptions? options,
        HopDecisionFlags outcome, CancellationToken ct = default)
    {
        var outType = output.GetDataType();

        if (output is IForwardOnlyDataStream)
        {
            var wrapDel = WrapJoinOutputsDelegates.GetOrAdd(outType, static t => BuildWrapJoinOutputsDelegate(t));

            var rawAsync = output.ToAsyncEnumerable(ct);

            var castMethod =
                typeof(LineageService).GetMethod(nameof(CastAsyncEnumerable), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(outType);

            var typedAsync = castMethod.Invoke(null, [rawAsync])!;
            var wrappedStream = wrapDel(typedAsync, currentNodeId, pipelineId, pipelineName, options, outcome, ct);
            var lineagePipeType = typeof(DataStream<>).MakeGenericType(typeof(LineagePacket<>).MakeGenericType(outType));
            return (IDataStream)Activator.CreateInstance(lineagePipeType, wrappedStream, $"LineageWrapped_{currentNodeId}")!;
        }

        var enumerableData = ExtractDataFromPipe(output);

        var toAsyncDel = EnumerableToAsyncDelegates.GetOrAdd(outType, static t => BuildEnumerableToAsyncDelegate(t));
        var asyncEnumerable = toAsyncDel(enumerableData);

        var wrapDelegate = WrapJoinOutputsDelegates.GetOrAdd(outType, static t => BuildWrapJoinOutputsDelegate(t));
        var wrappedAsync = wrapDelegate(asyncEnumerable, currentNodeId, pipelineId, pipelineName, options, outcome, ct);
        var pipeType = typeof(DataStream<>).MakeGenericType(typeof(LineagePacket<>).MakeGenericType(outType));
        return (IDataStream)Activator.CreateInstance(pipeType, wrappedAsync, $"LineageWrapped_{currentNodeId}")!;
    }

    /// <inheritdoc />
    public IDataStream WrapNodeOutputFromInputLineage(
        IDataStream output,
        IAsyncEnumerable<object?> inputLineageContext,
        string currentNodeId,
        Guid pipelineId,
        string? pipelineName,
        LineageOptions? options,
        HopDecisionFlags outcome,
        Type? lineageMapperType = null,
        CancellationToken ct = default)
    {
        var outType = output.GetDataType();
        var wrapDel = WrapNodeOutputsFromInputDelegates.GetOrAdd(outType, static t => BuildWrapNodeOutputFromInputDelegate(t));

        var rawAsync = output.ToAsyncEnumerable(ct);

        var castMethod =
            typeof(LineageService).GetMethod(nameof(CastAsyncEnumerable), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(outType);

        var typedAsync = castMethod.Invoke(null, [rawAsync])!;

        var wrappedStream = wrapDel(typedAsync, inputLineageContext, currentNodeId, pipelineId, pipelineName, options, outcome, lineageMapperType, ct);
        var lineagePipeType = typeof(DataStream<>).MakeGenericType(typeof(LineagePacket<>).MakeGenericType(outType));
        return (IDataStream)Activator.CreateInstance(lineagePipeType, wrappedStream, $"LineageMapped_{currentNodeId}")!;
    }

    private static async Task PumpInputWithLineageContextAsync(
        IAsyncEnumerable<object?> source,
        ChannelWriter<object?> dataWriter,
        ChannelWriter<object?> contextWriter,
        CancellationToken ct)
    {
        try
        {
            await foreach (var item in source.WithCancellation(ct).ConfigureAwait(false))
            {
                if (item is ILineageEnvelope envelope)
                {
                    await contextWriter.WriteAsync(item, ct).ConfigureAwait(false);
                    await dataWriter.WriteAsync(envelope.Data, ct).ConfigureAwait(false);
                    continue;
                }

                await contextWriter.WriteAsync(new RawInputContext(item), ct).ConfigureAwait(false);
                await dataWriter.WriteAsync(item, ct).ConfigureAwait(false);
            }

            contextWriter.TryComplete();
            dataWriter.TryComplete();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            contextWriter.TryComplete();
            dataWriter.TryComplete();
        }
        catch (Exception ex)
        {
            contextWriter.TryComplete(ex);
            dataWriter.TryComplete(ex);
        }
    }

    private static IAsyncEnumerable<T> CastAsyncEnumerable<T>(IAsyncEnumerable<object?> source)
    {
        return CastImpl();

        async IAsyncEnumerable<T> CastImpl([EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var obj in source.WithCancellation(ct).ConfigureAwait(false))
            {
                if (obj is T t)
                    yield return t;
                else
                    yield return (T)obj!;
            }
        }
    }

    private static object UnwrapIfLineage(object item)
    {
        return item is ILineageEnvelope env
            ? env.Data!
            : item;
    }

    private static Func<IDataStream, string, Guid, string?, LineageOptions?, IDataStream> BuildWrapSourceDelegate(Type dataType)
    {
        var method =
            typeof(LineageService).GetMethod(nameof(WrapSourceStreamGeneric), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(dataType);

        var pipeParam = Expression.Parameter(typeof(IDataStream), "pipe");
        var nodeIdParam = Expression.Parameter(typeof(string), "nodeId");
        var pipelineIdParam = Expression.Parameter(typeof(Guid), "pipelineId");
        var pipelineNameParam = Expression.Parameter(typeof(string), "pipelineName");
        var optsParam = Expression.Parameter(typeof(LineageOptions), "options");
        var castPipe = Expression.Convert(pipeParam, typeof(IDataStream<>).MakeGenericType(dataType));
        var call = Expression.Call(method, castPipe, nodeIdParam, pipelineIdParam, pipelineNameParam, optsParam);
        var castResult = Expression.Convert(call, typeof(IDataStream));

        var lambda = Expression.Lambda<Func<IDataStream, string, Guid, string?, LineageOptions?, IDataStream>>(castResult, pipeParam, nodeIdParam,
            pipelineIdParam, pipelineNameParam, optsParam);

        return lambda.Compile();
    }

    private static DataStream<LineagePacket<T>> WrapSourceStreamGeneric<T>(IDataStream<T> sourcePipe, string nodeId, Guid pipelineId,
        string? pipelineName, LineageOptions? options)
    {
        return new DataStream<LineagePacket<T>>(WrapStream(CancellationToken.None), $"LineageWrapped_{sourcePipe.StreamName}");

        async IAsyncEnumerable<LineagePacket<T>> WrapStream([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var item in sourcePipe.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var correlationId = Guid.NewGuid();
                var collect = ShouldCollect(correlationId, options);

                yield return new LineagePacket<T>(item, correlationId, [$"{pipelineId:N}::{nodeId}"])
                {
                    Collect = collect,
                };
            }
        }
    }

    private static bool ShouldCollect(Guid correlationId, LineageOptions? options)
    {
        if (options is null || options.SampleEvery <= 1)
            return true;

        var mod = options.SampleEvery;

        if (mod <= 0)
            return true;

        var hash = correlationId.GetHashCode() & int.MaxValue;
        return hash % mod == 0;
    }

    private static Func<object, string, Guid, string?, LineageOptions?, HopDecisionFlags, CancellationToken, object> BuildWrapJoinOutputsDelegate(
        Type outType)
    {
        var method = typeof(LineageService).GetMethod(nameof(WrapJoinOutputsGeneric), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(outType);
        var outputParam = Expression.Parameter(typeof(object), "output");
        var nodeParam = Expression.Parameter(typeof(string), "nodeId");
        var pipelineIdParam = Expression.Parameter(typeof(Guid), "pipelineId");
        var pipelineNameParam = Expression.Parameter(typeof(string), "pipelineName");
        var optsParam = Expression.Parameter(typeof(LineageOptions), "options");
        var outcomeParam = Expression.Parameter(typeof(HopDecisionFlags), "outcome");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");
        var castOutput = Expression.Convert(outputParam, typeof(IAsyncEnumerable<>).MakeGenericType(outType));
        var call = Expression.Call(method, castOutput, nodeParam, pipelineIdParam, pipelineNameParam, optsParam, outcomeParam, ctParam);
        var castResult = Expression.Convert(call, typeof(object));

        var lambda = Expression.Lambda<Func<object, string, Guid, string?, LineageOptions?, HopDecisionFlags, CancellationToken, object>>(castResult,
            outputParam, nodeParam, pipelineIdParam, pipelineNameParam, optsParam, outcomeParam, ctParam);

        return lambda.Compile();
    }

    private static Func<object, IAsyncEnumerable<object?>, string, Guid, string?, LineageOptions?, HopDecisionFlags, Type?, CancellationToken, object>
        BuildWrapNodeOutputFromInputDelegate(Type outType)
    {
        var method = typeof(LineageService)
            .GetMethod(nameof(WrapNodeOutputsFromInputLineageGeneric), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(outType);

        var outputParam = Expression.Parameter(typeof(object), "output");
        var inputCtxParam = Expression.Parameter(typeof(IAsyncEnumerable<object?>), "inputContext");
        var nodeParam = Expression.Parameter(typeof(string), "nodeId");
        var pipelineIdParam = Expression.Parameter(typeof(Guid), "pipelineId");
        var pipelineNameParam = Expression.Parameter(typeof(string), "pipelineName");
        var optsParam = Expression.Parameter(typeof(LineageOptions), "options");
        var outcomeParam = Expression.Parameter(typeof(HopDecisionFlags), "outcome");
        var mapperTypeParam = Expression.Parameter(typeof(Type), "mapperType");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        var castOutput = Expression.Convert(outputParam, typeof(IAsyncEnumerable<>).MakeGenericType(outType));

        var call = Expression.Call(method, castOutput, inputCtxParam, nodeParam, pipelineIdParam, pipelineNameParam, optsParam, outcomeParam,
            mapperTypeParam, ctParam);

        var castResult = Expression.Convert(call, typeof(object));

        var lambda = Expression.Lambda<Func<object, IAsyncEnumerable<object?>, string, Guid, string?, LineageOptions?, HopDecisionFlags, Type?,
            CancellationToken, object>>(castResult, outputParam, inputCtxParam, nodeParam, pipelineIdParam, pipelineNameParam, optsParam, outcomeParam,
            mapperTypeParam, ctParam);

        return lambda.Compile();
    }

    private static async IAsyncEnumerable<LineagePacket<TOut>> WrapNodeOutputsFromInputLineageGeneric<TOut>(
        IAsyncEnumerable<TOut> output,
        IAsyncEnumerable<object?> inputLineageContext,
        string currentNodeId,
        Guid pipelineId,
        string? pipelineName,
        LineageOptions? options,
        HopDecisionFlags outcome,
        Type? mapperType,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var inputEntries = new List<InputLineageEntry>();

        await foreach (var inputContext in inputLineageContext.WithCancellation(ct).ConfigureAwait(false))
        {
            inputEntries.Add(ToInputLineageEntry(inputContext));
        }

        var mapper = ResolveMapper(mapperType);
        var shouldMaterializeOutputs = mapper is not null || ShouldMaterializeJoinAggregate(inputEntries.Count, outcome, options);

        if (shouldMaterializeOutputs)
        {
            var outputs = new List<TOut>();

            await foreach (var item in output.WithCancellation(ct).ConfigureAwait(false))
            {
                outputs.Add(item);
            }

            var mapperRecords = mapper is null
                ? null
                : BuildMapperRecords(inputEntries, outputs, mapper, currentNodeId);

            foreach (var packet in MapMaterializedNodeOutputs(inputEntries, outputs, mapperRecords, currentNodeId, pipelineId, pipelineName, options,
                         outcome))
            {
                yield return packet;
            }

            yield break;
        }

        var contributorCount = inputEntries.Count;

        int[] representativeIndices = contributorCount > 0
            ? [0]
            : [];

        await foreach (var outputItem in output.WithCancellation(ct).ConfigureAwait(false))
        {
            var packet = BuildPacketFromContributors(outputItem, inputEntries, representativeIndices, contributorCount, currentNodeId, pipelineId,
                pipelineName, options, outcome, null);

            yield return packet;
        }
    }

    private static bool ShouldMaterializeJoinAggregate(int inputCount, HopDecisionFlags outcome, LineageOptions? options)
    {
        if ((outcome & (HopDecisionFlags.Joined | HopDecisionFlags.Aggregated)) == 0)
            return false;

        var cap = options?.MaterializationCap;

        return cap is > 0
            ? inputCount <= cap.Value
            : inputCount <= SmallMaterializationThreshold;
    }

    private static Dictionary<int, int[]> BuildMapperRecords<TOut>(
        IReadOnlyList<InputLineageEntry> inputs,
        IReadOnlyList<TOut> outputs,
        ILineageMapper mapper,
        string nodeId)
    {
        List<object> mapperInputs = [];

        foreach (var input in inputs)
        {
            mapperInputs.Add((input.OriginalPacket ?? input.Data)!);
        }

        List<object> mapperOutputs = [];

        foreach (var output in outputs)
        {
            mapperOutputs.Add(output!);
        }

        var mapping = mapper.MapInputToOutputs(mapperInputs, mapperOutputs, new LineageMappingContext(nodeId));
        var records = new Dictionary<int, int[]>();

        foreach (var record in mapping.Records)
        {
            var normalized = record.InputIndices
                .Where(i => i >= 0 && i < inputs.Count)
                .Distinct()
                .OrderBy(i => i)
                .ToArray();

            records[record.OutputIndex] = normalized;
        }

        return records;
    }

    private static IEnumerable<LineagePacket<TOut>> MapMaterializedNodeOutputs<TOut>(
        IReadOnlyList<InputLineageEntry> inputs,
        IReadOnlyList<TOut> outputs,
        IReadOnlyDictionary<int, int[]>? mapperRecords,
        string currentNodeId,
        Guid pipelineId,
        string? pipelineName,
        LineageOptions? options,
        HopDecisionFlags outcome)
    {
        var inputCount = inputs.Count;
        var outputCount = outputs.Count;

        var allInputIndices = inputCount > 0
            ? Enumerable.Range(0, inputCount).ToArray()
            : [];

        var contributorByOutput = new (int[] ContributorIndices, int ContributorCount)[outputCount];
        var outputCountByInput = new Dictionary<int, int>();

        for (var outputIndex = 0; outputIndex < outputCount; outputIndex++)
        {
            int[] contributorIndices;
            int contributorCount;

            if (mapperRecords is not null && mapperRecords.TryGetValue(outputIndex, out var mapped))
            {
                contributorIndices = mapped;
                contributorCount = mapped.Length;
            }
            else if (inputCount > 0 && inputCount == outputCount)
            {
                contributorIndices = [outputIndex];
                contributorCount = 1;
            }
            else if (inputCount > 0)
            {
                contributorIndices = allInputIndices;
                contributorCount = inputCount;
            }
            else
            {
                contributorIndices = [];
                contributorCount = 0;
            }

            contributorByOutput[outputIndex] = (contributorIndices, contributorCount);

            foreach (var contributorIndex in contributorIndices)
            {
                if (contributorIndex < 0 || contributorIndex >= inputCount)
                    continue;

                outputCountByInput[contributorIndex] = outputCountByInput.TryGetValue(contributorIndex, out var count)
                    ? count + 1
                    : 1;
            }
        }

        for (var outputIndex = 0; outputIndex < outputCount; outputIndex++)
        {
            var (contributorIndices, contributorCount) = contributorByOutput[outputIndex];
            var outputEmissionCount = ResolveOutputEmissionCount(contributorIndices, outputCountByInput, inputCount);

            yield return BuildPacketFromContributors(outputs[outputIndex], inputs, contributorIndices, contributorCount, currentNodeId, pipelineId,
                pipelineName, options, outcome, outputEmissionCount);
        }
    }

    private static int? ResolveOutputEmissionCount(
        IReadOnlyList<int> contributorIndices,
        IReadOnlyDictionary<int, int> outputCountByInput,
        int inputCount)
    {
        if (contributorIndices.Count == 0)
            return null;

        int? resolved = null;

        foreach (var contributorIndex in contributorIndices)
        {
            if (contributorIndex < 0 || contributorIndex >= inputCount)
                continue;

            if (!outputCountByInput.TryGetValue(contributorIndex, out var contributorOutputCount))
                continue;

            if (resolved is null)
            {
                resolved = contributorOutputCount;
                continue;
            }

            if (resolved.Value != contributorOutputCount)
            {
                // A single scalar cannot represent conflicting fan-out counts across contributors.
                return null;
            }
        }

        return resolved;
    }

    private static LineagePacket<TOut> BuildPacketFromContributors<TOut>(
        TOut outputData,
        IReadOnlyList<InputLineageEntry> inputs,
        IReadOnlyList<int> contributorIndices,
        int contributorCount,
        string currentNodeId,
        Guid pipelineId,
        string? pipelineName,
        LineageOptions? options,
        HopDecisionFlags outcome,
        int? outputEmissionCount)
    {
        if (contributorIndices.Count == 0 || contributorCount <= 0)
            return CreateFreshPacket(outputData, currentNodeId, pipelineId, pipelineName, options, outcome, 0, null, outputEmissionCount);

        List<InputLineageEntry> lineageContributors = [];

        foreach (var contributorIndex in contributorIndices)
        {
            if (contributorIndex < 0 || contributorIndex >= inputs.Count)
                continue;

            var entry = inputs[contributorIndex];

            if (entry.HasLineage)
                lineageContributors.Add(entry);
        }

        if (lineageContributors.Count == 0)
        {
            return CreateFreshPacket(outputData, currentNodeId, pipelineId, pipelineName, options, outcome, contributorCount, contributorIndices,
                outputEmissionCount);
        }

        var representative = lineageContributors[0];
        var traversalPath = MergeTraversalPath(lineageContributors).Add(QualifyNodeId(currentNodeId, pipelineId));

        var hopRecords = representative.LineageHops;

        if (representative.Collect)
        {
            hopRecords = AppendOutcomeHop(hopRecords, currentNodeId, pipelineId, pipelineName, options, outcome, contributorCount,
                contributorIndices, outputEmissionCount, representative.Data, outputData);
        }

        return new LineagePacket<TOut>(outputData!, representative.CorrelationId, traversalPath)
        {
            Collect = representative.Collect,
            LineageHops = hopRecords,
        };
    }

    private static LineagePacket<TOut> CreateFreshPacket<TOut>(
        TOut outputData,
        string currentNodeId,
        Guid pipelineId,
        string? pipelineName,
        LineageOptions? options,
        HopDecisionFlags outcome,
        int contributorCount,
        IReadOnlyList<int>? contributorIndices,
        int? outputEmissionCount)
    {
        var correlationId = Guid.NewGuid();
        var collect = ShouldCollect(correlationId, options);

        var hopRecords = ImmutableList<LineageHop>.Empty;

        if (collect)
        {
            hopRecords = AppendOutcomeHop(hopRecords, currentNodeId, pipelineId, pipelineName, options, outcome, contributorCount,
                contributorIndices, outputEmissionCount, null, outputData);
        }

        return new LineagePacket<TOut>(outputData!, correlationId, [QualifyNodeId(currentNodeId, pipelineId)])
        {
            Collect = collect,
            LineageHops = hopRecords,
        };
    }

    private static ImmutableList<LineageHop> AppendOutcomeHop(
        ImmutableList<LineageHop> existing,
        string nodeId,
        Guid pipelineId,
        string? pipelineName,
        LineageOptions? options,
        HopDecisionFlags outcome,
        int contributorCount,
        IReadOnlyList<int>? contributorIndices,
        int? outputEmissionCount,
        object? inputSnapshot,
        object? outputSnapshot)
    {
        var cap = options is not null && options.MaxHopRecordsPerItem > 0
            ? options.MaxHopRecordsPerItem
            : int.MaxValue;

        if (existing.Count >= cap)
            return existing;

        var truncated = existing.Count + 1 >= cap;

        var cardinality = contributorCount switch
        {
            <= 0 => ObservedCardinality.Zero,
            1 => ObservedCardinality.One,
            _ => ObservedCardinality.Many,
        };

        int? contributorField = options?.CaptureObservedCardinality == false
            ? null
            : contributorCount;

        var outputEmissionField = options?.CaptureObservedCardinality == false
            ? null
            : outputEmissionCount;

        var ancestryField = options?.CaptureAncestryMapping == true
            ? contributorIndices
            : null;

        var hop = new LineageHop(
            nodeId,
            outcome,
            cardinality,
            contributorField,
            outputEmissionField,
            ancestryField,
            truncated,
            pipelineId,
            SnapshotValue(inputSnapshot, options),
            SnapshotValue(outputSnapshot, options),
            pipelineName);

        return existing.Add(hop);
    }

    private static ImmutableList<string> MergeTraversalPath(IReadOnlyList<InputLineageEntry> contributors)
    {
        var merged = contributors[0].TraversalPath;

        if (contributors.Count == 1)
            return merged;

        var seen = new HashSet<string>(merged, StringComparer.Ordinal);

        for (var i = 1; i < contributors.Count; i++)
        {
            foreach (var segment in contributors[i].TraversalPath)
            {
                if (seen.Add(segment))
                    merged = merged.Add(segment);
            }
        }

        return merged;
    }

    private static string QualifyNodeId(string nodeId, Guid pipelineId)
    {
        return $"{pipelineId:N}::{nodeId}";
    }

    private static InputLineageEntry ToInputLineageEntry(object? context)
    {
        if (context is ILineageEnvelope envelope)
        {
            // ReSharper disable once RedundantCast - Without the cast, we'd get a runtime error trying to spread null.
            var traversal = envelope.TraversalPath as ImmutableList<string> ?? [.. envelope.TraversalPath];
            var hops = envelope.LineageHops as ImmutableList<LineageHop> ?? [.. envelope.LineageHops];
            // ReSharper restore RedundantCast

            return new InputLineageEntry(envelope.Data, envelope.CorrelationId, traversal, hops, envelope.Collect, true, context);
        }

        if (context is RawInputContext raw)
            return new InputLineageEntry(raw.Data, Guid.Empty, [], [], true, false, null);

        return new InputLineageEntry(context, Guid.Empty, [], [], true, false, null);
    }

    private static ILineageMapper? ResolveMapper(Type? mapperType)
    {
        if (mapperType is null || !typeof(ILineageMapper).IsAssignableFrom(mapperType))
            return null;

        return MapperInstances.GetOrAdd(mapperType, static t => (ILineageMapper)Activator.CreateInstance(t)!);
    }

    private static async IAsyncEnumerable<LineagePacket<T>> WrapJoinOutputsGeneric<T>(
        IAsyncEnumerable<T> output,
        string currentNodeId,
        Guid pipelineId,
        string? pipelineName,
        LineageOptions? options,
        HopDecisionFlags outcome,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        await foreach (var item in output.WithCancellation(ct).ConfigureAwait(false))
        {
            var correlationId = Guid.NewGuid();
            var collect = ShouldCollect(correlationId, options);
            var hopRecords = ImmutableList<LineageHop>.Empty;

            if (collect)
            {
                var cap = options != null && options.MaxHopRecordsPerItem > 0
                    ? options.MaxHopRecordsPerItem
                    : int.MaxValue;

                if (cap > 0)
                {
                    var seed = new LineageHop(
                        currentNodeId,
                        outcome,
                        ObservedCardinality.Unknown,
                        null,
                        null,
                        null,
                        false,
                        pipelineId,
                        null,
                        SnapshotValue(item, options),
                        pipelineName);

                    hopRecords = hopRecords.Add(seed);
                }
            }

            yield return new LineagePacket<T>(item!, correlationId, [$"{pipelineId:N}::{currentNodeId}"])
            {
                Collect = collect,
                LineageHops = hopRecords,
            };
        }
    }

    private static object? SnapshotValue(object? value, LineageOptions? options)
    {
        return LineageMappingStrategyBase.SnapshotValue(value, options);
    }

    private static IEnumerable ExtractDataFromPipe(IDataStream pipe)
    {
        return pipe switch
        {
            var listPipe when listPipe.GetType().IsGenericType &&
                              listPipe.GetType().GetGenericTypeDefinition() == typeof(InMemoryDataStream<>) =>
                listPipe.GetType().GetProperty("Items")?.GetValue(listPipe) as IEnumerable ??
                throw new InvalidOperationException("Failed to extract Items from InMemoryDataStream"),

            _ => MaterializePipeData(pipe),
        };
    }

    private static List<object?> MaterializePipeData(IDataStream pipe)
    {
        var asyncEnum = pipe.ToAsyncEnumerable(CancellationToken.None);
        var list = new List<object?>();

        var task = Task.Run(async () =>
        {
            await foreach (var item in asyncEnum.WithCancellation(CancellationToken.None).ConfigureAwait(false))
            {
                list.Add(item);
            }
        });

        task.GetAwaiter().GetResult();

        return list;
    }

    private static Func<IEnumerable, object> BuildEnumerableToAsyncDelegate(Type t)
    {
        var method = typeof(LineageService).GetMethod(nameof(EnumerableToAsync), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(t);
        var srcParam = Expression.Parameter(typeof(IEnumerable), "src");
        var call = Expression.Call(method, srcParam);
        var castResult = Expression.Convert(call, typeof(object));
        var lambda = Expression.Lambda<Func<IEnumerable, object>>(castResult, srcParam);
        return lambda.Compile();
    }

    private static async IAsyncEnumerable<T> EnumerableToAsync<T>(
        IEnumerable source,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        foreach (var item in source)
        {
            ct.ThrowIfCancellationRequested();

            if (item is T t)
                yield return t;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    private sealed record RawInputContext(object? Data);

    private sealed record InputLineageEntry(
        object? Data,
        Guid CorrelationId,
        ImmutableList<string> TraversalPath,
        ImmutableList<LineageHop> LineageHops,
        bool Collect,
        bool HasLineage,
        object? OriginalPacket);
}
