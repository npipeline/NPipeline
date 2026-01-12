using NPipeline.Pipeline;
using Sample_LineageExtension.Nodes;

namespace Sample_LineageExtension
{
    /// <summary>
    ///     Basic lineage tracking pipeline demonstrating fundamental lineage concepts.
    ///     Shows how lineage is tracked through each node in a simple pipeline.
    /// </summary>
    public sealed class BasicLineageTrackingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var orderSource = builder.AddSource<OrderSource, OrderEvent>("order-source");
            var enrichment = builder.AddTransform<EnrichmentTransform, OrderEvent, EnrichedOrder>("enrichment");
            var validation = builder.AddTransform<ValidationTransform, EnrichedOrder, ValidatedOrder>("validation");
            var processing = builder.AddTransform<ProcessingTransform, ValidatedOrder, ProcessedOrder>("processing");
            var consoleSink = builder.AddSink<ConsoleSink, ProcessedOrder>("console-sink");

            _ = builder.Connect(orderSource, enrichment);
            _ = builder.Connect(enrichment, validation);
            _ = builder.Connect(validation, processing);
            _ = builder.Connect(processing, consoleSink);
        }
    }

    /// <summary>
    ///     Pipeline demonstrating deterministic sampling for lineage tracking.
    ///     Shows how to sample every N items to reduce overhead while maintaining visibility.
    /// </summary>
    public sealed class DeterministicSamplingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var orderSource = builder.AddSource<OrderSource, OrderEvent>("order-source");
            var enrichment = builder.AddTransform<EnrichmentTransform, OrderEvent, EnrichedOrder>("enrichment");
            var validation = builder.AddTransform<ValidationTransform, EnrichedOrder, ValidatedOrder>("validation");
            var processing = builder.AddTransform<ProcessingTransform, ValidatedOrder, ProcessedOrder>("processing");
            var consoleSink = builder.AddSink<ConsoleSink, ProcessedOrder>("console-sink");

            _ = builder.Connect(orderSource, enrichment);
            _ = builder.Connect(enrichment, validation);
            _ = builder.Connect(validation, processing);
            _ = builder.Connect(processing, consoleSink);
        }
    }

    /// <summary>
    ///     Pipeline demonstrating random (non-deterministic) sampling for lineage tracking.
    ///     Shows how to sample items randomly to reduce overhead.
    /// </summary>
    public sealed class RandomSamplingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var orderSource = builder.AddSource<OrderSource, OrderEvent>("order-source");
            var enrichment = builder.AddTransform<EnrichmentTransform, OrderEvent, EnrichedOrder>("enrichment");
            var validation = builder.AddTransform<ValidationTransform, EnrichedOrder, ValidatedOrder>("validation");
            var processing = builder.AddTransform<ProcessingTransform, ValidatedOrder, ProcessedOrder>("processing");
            var consoleSink = builder.AddSink<ConsoleSink, ProcessedOrder>("console-sink");

            _ = builder.Connect(orderSource, enrichment);
            _ = builder.Connect(enrichment, validation);
            _ = builder.Connect(validation, processing);
            _ = builder.Connect(processing, consoleSink);
        }
    }

    /// <summary>
    ///     Complex pipeline with join node demonstrating lineage across joins.
    ///     Shows how lineage is maintained when combining data from multiple sources.
    /// </summary>
    public sealed class ComplexJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var orderSource = builder.AddSource<OrderSource, OrderEvent>("order-source");
            var enrichment = builder.AddTransform<EnrichmentTransform, OrderEvent, EnrichedOrder>("enrichment");
            var validation = builder.AddTransform<ValidationTransform, EnrichedOrder, ValidatedOrder>("validation");
            var processing = builder.AddTransform<ProcessingTransform, ValidatedOrder, ProcessedOrder>("processing");
            var consoleSink = builder.AddSink<ConsoleSink, ProcessedOrder>("console-sink");

            _ = builder.Connect(orderSource, enrichment);
            _ = builder.Connect(enrichment, validation);
            _ = builder.Connect(validation, processing);
            _ = builder.Connect(processing, consoleSink);
        }
    }

    /// <summary>
    ///     Pipeline with branch node demonstrating lineage splitting and recombining.
    ///     Shows how lineage tracks items through different branches.
    /// </summary>
    public sealed class BranchingWithLineagePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var orderSource = builder.AddSource<OrderSource, OrderEvent>("order-source");
            var enrichment = builder.AddTransform<EnrichmentTransform, OrderEvent, EnrichedOrder>("enrichment");
            var fraudDetection = builder.AddTransform<FraudDetectionBranch, EnrichedOrder, EnrichedOrder>("fraud-detection");
            var validation = builder.AddTransform<ValidationTransform, EnrichedOrder, ValidatedOrder>("validation");
            var processing = builder.AddTransform<ProcessingTransform, ValidatedOrder, ProcessedOrder>("processing");
            var consoleSink = builder.AddSink<ConsoleSink, ProcessedOrder>("console-sink");

            _ = builder.Connect(orderSource, enrichment);
            _ = builder.Connect(enrichment, fraudDetection);
            _ = builder.Connect(fraudDetection, validation);
            _ = builder.Connect(validation, processing);
            _ = builder.Connect(processing, consoleSink);
        }
    }

    /// <summary>
    ///     Pipeline demonstrating error handling with lineage tracking.
    ///     Shows how lineage tracks error outcomes and retry scenarios.
    /// </summary>
    public sealed class ErrorHandlingWithLineagePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var orderSource = builder.AddSource<OrderSource, OrderEvent>("order-source");
            var enrichment = builder.AddTransform<EnrichmentTransform, OrderEvent, EnrichedOrder>("enrichment");
            var validation = builder.AddTransform<ValidationTransform, EnrichedOrder, ValidatedOrder>("validation");
            var processing = builder.AddTransform<ProcessingTransform, ValidatedOrder, ProcessedOrder>("processing");
            var consoleSink = builder.AddSink<ConsoleSink, ProcessedOrder>("console-sink");
            var databaseSink = builder.AddSink<DatabaseSink, ProcessedOrder>("database-sink");

            _ = builder.Connect(orderSource, enrichment);
            _ = builder.Connect(enrichment, validation);
            _ = builder.Connect(validation, processing);
            _ = builder.Connect(processing, consoleSink);
            _ = builder.Connect(processing, databaseSink);
        }
    }

    /// <summary>
    ///     Pipeline demonstrating custom lineage sink for exporting lineage data.
    ///     Shows how to implement and use custom IPipelineLineageSink.
    /// </summary>
    public sealed class CustomLineageSinkPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            _ = builder.AddPipelineLineageSink<CustomLineageSink>();

            var orderSource = builder.AddSource<OrderSource, OrderEvent>("order-source");
            var enrichment = builder.AddTransform<EnrichmentTransform, OrderEvent, EnrichedOrder>("enrichment");
            var validation = builder.AddTransform<ValidationTransform, EnrichedOrder, ValidatedOrder>("validation");
            var processing = builder.AddTransform<ProcessingTransform, ValidatedOrder, ProcessedOrder>("processing");
            var consoleSink = builder.AddSink<ConsoleSink, ProcessedOrder>("console-sink");

            _ = builder.Connect(orderSource, enrichment);
            _ = builder.Connect(enrichment, validation);
            _ = builder.Connect(validation, processing);
            _ = builder.Connect(processing, consoleSink);
        }
    }

    /// <summary>
    ///     Transform node that converts ValidatedOrder to ProcessedOrder.
    ///     This represents the final processing stage in the pipeline.
    /// </summary>
    public sealed class ProcessingTransform : NPipeline.Nodes.TransformNode<ValidatedOrder, ProcessedOrder>
    {
        public override Task<ProcessedOrder> ExecuteAsync(ValidatedOrder validatedOrder, PipelineContext context, CancellationToken cancellationToken)
        {
            var result = validatedOrder.IsValid ? ProcessingResult.Success : ProcessingResult.Failed;
            var notes = validatedOrder.IsValid ? null : $"Validation failed: {string.Join(", ", validatedOrder.ValidationErrors)}";

            var processedOrder = new ProcessedOrder(validatedOrder, result, notes);

            return Task.FromResult(processedOrder);
        }
    }

    /// <summary>
    ///     Enumeration of available demo scenarios.
    /// </summary>
    public enum DemoScenario
    {
        BasicLineageTracking,
        DeterministicSampling,
        RandomSampling,
        ComplexJoin,
        BranchingWithLineage,
        ErrorHandlingWithLineage,
        CustomLineageSink
    }
}