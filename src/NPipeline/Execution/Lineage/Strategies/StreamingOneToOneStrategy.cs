using System.Diagnostics;
using System.Runtime.CompilerServices;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.Lineage;

namespace NPipeline.Execution.Lineage.Strategies;

internal sealed class StreamingOneToOneStrategy<TIn, TOut> : ILineageMappingStrategy<TIn, TOut>
{
    public static readonly StreamingOneToOneStrategy<TIn, TOut> Instance = new();

    private StreamingOneToOneStrategy()
    {
    }

    public async IAsyncEnumerable<LineagePacket<TOut>> MapAsync(IAsyncEnumerable<LineagePacket<TIn>> inputStream, IAsyncEnumerable<TOut> outputStream,
        string nodeId, TransformCardinality cardinality, LineageOptions? options, Type? lineageMapperType, ILineageMapper? mapperInstance,
        [EnumeratorCancellation] CancellationToken ct)
    {
        // Fast streaming 1:1 path (original second half of BuildLineageAdapter)
        await using var inputEnumerator2 = inputStream.GetAsyncEnumerator(ct);
        await using var outputEnumerator2 = outputStream.GetAsyncEnumerator(ct);
        var matchedInputCount2 = 0;
        var matchedOutputCount2 = 0;

        while (true)
        {
            var hasInput = await inputEnumerator2.MoveNextAsync().ConfigureAwait(false);
            var hasOutput = await outputEnumerator2.MoveNextAsync().ConfigureAwait(false);

            if (hasInput && hasOutput)
            {
                var inputPacket = inputEnumerator2.Current;
                var outputData = outputEnumerator2.Current;
                var hopRecords = inputPacket.LineageHops;

                if (inputPacket.Collect)
                    hopRecords = LineageMappingHelpers.MaybeAppendHop(hopRecords, nodeId, options);

                yield return new LineagePacket<TOut>(outputData, inputPacket.LineageId, inputPacket.TraversalPath.Add(nodeId))
                    { Collect = inputPacket.Collect, LineageHops = hopRecords };

                matchedInputCount2++;
                matchedOutputCount2++;
                continue;
            }

            var totalIn2 = matchedInputCount2;
            var totalOut2 = matchedOutputCount2;

            if (hasInput)
            {
                totalIn2++;

                while (await inputEnumerator2.MoveNextAsync().ConfigureAwait(false))
                {
                    totalIn2++;
                }
            }

            if (hasOutput)
            {
                totalOut2++;

                while (await outputEnumerator2.MoveNextAsync().ConfigureAwait(false))
                {
                    totalOut2++;
                }
            }

            if (totalIn2 != totalOut2 && cardinality == TransformCardinality.OneToOne)
            {
                var missingInputs = new List<int>();
                var extraOutputs = new List<int>();

                if (totalIn2 > totalOut2)
                {
                    for (var mi = totalOut2; mi < totalIn2; mi++)
                    {
                        missingInputs.Add(mi);
                    }
                }
                else if (totalOut2 > totalIn2)
                {
                    for (var eo = totalIn2; eo < totalOut2; eo++)
                    {
                        extraOutputs.Add(eo);
                    }
                }

                var ctx = new LineageMismatchContext(nodeId, totalIn2, totalOut2, missingInputs, extraOutputs, []);
                options?.OnMismatch?.Invoke(ctx);

                if (options?.Strict == true)
                {
                    throw new InvalidOperationException(
                        $"Lineage cardinality mismatch in node '{nodeId}': inputs={totalIn2}, outputs={totalOut2}. Declare intended cardinality with [TransformCardinality] or adjust transform for 1:1.");
                }

                if (options?.WarnOnMismatch == true)
                {
                    Trace.TraceWarning(
                        $"[NPipeline.Lineage] Node {nodeId} 1:1 mismatch detected (in={totalIn2}, out={totalOut2}). MissingInputs={missingInputs.Count} ExtraOutputs={extraOutputs.Count}");
                }
            }

            break;
        }
    }
}
