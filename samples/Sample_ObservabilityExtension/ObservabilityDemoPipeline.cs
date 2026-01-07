using NPipeline.Pipeline;
using Sample_ObservabilityExtension.Nodes;

namespace Sample_ObservabilityExtension;

/// <summary>
///     Demo pipeline showcasing the observability extension capabilities.
///     This pipeline processes numeric data through multiple stages, generating
///     comprehensive metrics that are collected and displayed.
/// </summary>
public class ObservabilityDemoPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure with observability metrics collection.
    /// </summary>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that generates numeric data
        var source = builder.AddSource<NumberGenerator, int>("number-generator");

        // Add the first transform that doubles numbers
        var doubler = builder.AddTransform<NumberFilter, int, int>("number-doubler");

        // Add the second transform that doubles again
        var doubler2 = builder.AddTransform<NumberMultiplier, int, int>("number-quadrupler");

        // Add the sink that outputs results
        var sink = builder.AddSink<ResultAggregator, int>("result-aggregator");

        // Connect the nodes in sequence
        builder.Connect(source, doubler);
        builder.Connect(doubler, doubler2);
        builder.Connect(doubler2, sink);
    }

    /// <summary>
    ///     Gets a description of the pipeline.
    /// </summary>
    public static string GetDescription()
    {
        return @"Observability Extension Demo Pipeline:

This sample demonstrates the comprehensive observability features of NPipeline:

PIPELINE STRUCTURE:
1. NumberGenerator: Produces a sequence of numeric values (1-100)
2. NumberDoubler: Doubles each number (×2)
3. NumberQuadrupler: Doubles again (×2), resulting in 4× multiplier
4. ResultAggregator: Collects and outputs final results

OBSERVABILITY METRICS COLLECTED:
- Node execution duration (in milliseconds)
- Items processed and emitted at each stage
- Thread information for each node
- Success/failure status
- Performance metrics (throughput, average processing time)
- Retry attempts (if any)
- Memory usage (peak usage, initial memory)

DATA FLOW EXAMPLE:
- Input: Numbers 1-100
- Doubler Output: Each number × 2 (2, 4, 6, ..., 200) = 100 items
- Quadrupler Output: Each number × 4 (4, 8, 12, ..., 400) = 100 items
- Final Output: Aggregated results with statistics

METRICS INTERPRETATION:
- Items Processed: Total items received at each node
- Items Emitted: Total items sent to next node (should match for simple transforms)
- Throughput: Items per second processed
- Duration: Total time spent in the node (includes processing and I/O)
- Success: Whether the node completed without exceptions";
    }
}
