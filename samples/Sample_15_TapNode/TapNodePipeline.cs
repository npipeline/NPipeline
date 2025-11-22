using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NPipeline.Pipeline;
using Sample_15_TapNode.Nodes;

namespace Sample_15_TapNode;

/// <summary>
///     Pipeline definition that demonstrates TapNode functionality for non-intrusive monitoring.
///     This pipeline shows how to tap into data streams at various points for audit logging,
///     metrics collection, and alert generation without affecting main processing flow.
/// </summary>
public sealed class TapNodePipeline : IPipelineDefinition
{
    /// <inheritdoc />
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Main processing nodes
        var sourceHandle = builder.AddSource<TransactionSource, Transaction>("source");
        var validationHandle = builder.AddTransform<TransactionValidationTransform, Transaction, ValidatedTransaction>("validation");
        var riskAssessmentHandle = builder.AddTransform<RiskAssessmentTransform, ValidatedTransaction, ProcessedTransaction>("riskAssessment");
        var consoleSinkHandle = builder.AddSink<ConsoleSink, ProcessedTransaction>("consoleSink");

        // Tap nodes for monitoring and side-channel processing
        var sourceAuditTapHandle = builder.AddTap<Transaction>(() =>
            new AuditLogSink(NullLogger<AuditLogSink>.Instance, "SourceStage"), "sourceAuditTap");

        var sourceMetricsTapHandle = builder.AddTap<Transaction>(() =>
            new MetricsCollectionSink(NullLogger<MetricsCollectionSink>.Instance, "SourceStage"), "sourceMetricsTap");

        var sourceAlertTapHandle = builder.AddTap<Transaction>(() =>
            new AlertGenerationSink(NullLogger<AlertGenerationSink>.Instance, "SourceStage"), "sourceAlertTap");

        var validationAuditTapHandle = builder.AddTap<ValidatedTransaction>(() =>
            new AuditLogSink(NullLogger<AuditLogSink>.Instance, "ValidationStage"), "validationAuditTap");

        var validationMetricsTapHandle = builder.AddTap<ValidatedTransaction>(() =>
            new MetricsCollectionSink(NullLogger<MetricsCollectionSink>.Instance, "ValidationStage"), "validationMetricsTap");

        var validationAlertTapHandle = builder.AddTap<ValidatedTransaction>(() =>
            new AlertGenerationSink(NullLogger<AlertGenerationSink>.Instance, "ValidationStage"), "validationAlertTap");

        var processingAuditTapHandle = builder.AddTap<ProcessedTransaction>(() =>
            new AuditLogSink(NullLogger<AuditLogSink>.Instance, "ProcessingStage"), "processingAuditTap");

        var processingMetricsTapHandle = builder.AddTap<ProcessedTransaction>(() =>
            new MetricsCollectionSink(NullLogger<MetricsCollectionSink>.Instance, "ProcessingStage"), "processingMetricsTap");

        var processingAlertTapHandle = builder.AddTap<ProcessedTransaction>(() =>
            new AlertGenerationSink(NullLogger<AlertGenerationSink>.Instance, "ProcessingStage"), "processingAlertTap");

        // Connect main processing pipeline
        builder.Connect(sourceHandle, sourceAuditTapHandle);
        builder.Connect(sourceAuditTapHandle, sourceMetricsTapHandle);
        builder.Connect(sourceMetricsTapHandle, sourceAlertTapHandle);
        builder.Connect(sourceAlertTapHandle, validationHandle);

        builder.Connect(validationHandle, validationAuditTapHandle);
        builder.Connect(validationAuditTapHandle, validationMetricsTapHandle);
        builder.Connect(validationMetricsTapHandle, validationAlertTapHandle);
        builder.Connect(validationAlertTapHandle, riskAssessmentHandle);

        builder.Connect(riskAssessmentHandle, processingAuditTapHandle);
        builder.Connect(processingAuditTapHandle, processingMetricsTapHandle);
        builder.Connect(processingMetricsTapHandle, processingAlertTapHandle);
        builder.Connect(processingAlertTapHandle, consoleSinkHandle);
    }

    /// <summary>
    ///     Gets a description of tap node pipeline.
    /// </summary>
    /// <returns>A description of what this pipeline demonstrates.</returns>
    public static string GetDescription()
    {
        return """
               TapNode Pipeline Demonstration
               ============================

               This pipeline demonstrates the power of TapNode for non-intrusive monitoring and side-channel processing:

               1. **Main Processing Flow**:
                  - TransactionSource generates realistic financial transactions
                  - TransactionValidationTransform validates business rules
                  - RiskAssessmentTransform performs comprehensive risk analysis
                  - ConsoleSink displays final processing results

               2. **TapNode Monitoring Points**:
                  - **Source Tap**: Captures all incoming transactions for audit logging
                  - **Validation Tap**: Monitors validation results and failed transactions
                  - **Processing Tap**: Tracks final processing outcomes and performance metrics
                  - **Alert Tap**: Generates alerts for suspicious activities and performance issues

               3. **Key TapNode Benefits Demonstrated**:
                  - **Non-Intrusive**: Main processing continues uninterrupted
                  - **Multiple Tap Points**: Monitor at different pipeline stages
                  - **Side Effects**: Audit logging, metrics collection, alert generation
                  - **Error Isolation**: Tap failures don't affect main pipeline
                  - **Performance Monitoring**: Track processing times and bottlenecks

               The pipeline shows how TapNode enables comprehensive observability without modifying core business logic.
               """;
    }

    /// <summary>
    ///     Creates audit sink for source stage.
    /// </summary>
    private static AuditLogSink CreateSourceAuditSink(IServiceProvider sp)
    {
        return new AuditLogSink(sp.GetRequiredService<ILogger<AuditLogSink>>(), "SourceStage");
    }

    /// <summary>
    ///     Creates metrics sink for source stage.
    /// </summary>
    private static MetricsCollectionSink CreateSourceMetricsSink(IServiceProvider sp)
    {
        return new MetricsCollectionSink(sp.GetRequiredService<ILogger<MetricsCollectionSink>>(), "SourceStage");
    }

    /// <summary>
    ///     Creates alert sink for source stage.
    /// </summary>
    private static AlertGenerationSink CreateSourceAlertSink(IServiceProvider sp)
    {
        return new AlertGenerationSink(sp.GetRequiredService<ILogger<AlertGenerationSink>>(), "SourceStage");
    }

    /// <summary>
    ///     Creates audit sink for validation stage.
    /// </summary>
    private static AuditLogSink CreateValidationAuditSink(IServiceProvider sp)
    {
        return new AuditLogSink(sp.GetRequiredService<ILogger<AuditLogSink>>(), "ValidationStage");
    }

    /// <summary>
    ///     Creates metrics sink for validation stage.
    /// </summary>
    private static MetricsCollectionSink CreateValidationMetricsSink(IServiceProvider sp)
    {
        return new MetricsCollectionSink(sp.GetRequiredService<ILogger<MetricsCollectionSink>>(), "ValidationStage");
    }

    /// <summary>
    ///     Creates alert sink for validation stage.
    /// </summary>
    private static AlertGenerationSink CreateValidationAlertSink(IServiceProvider sp)
    {
        return new AlertGenerationSink(sp.GetRequiredService<ILogger<AlertGenerationSink>>(), "ValidationStage");
    }

    /// <summary>
    ///     Creates audit sink for processing stage.
    /// </summary>
    private static AuditLogSink CreateProcessingAuditSink(IServiceProvider sp)
    {
        return new AuditLogSink(sp.GetRequiredService<ILogger<AuditLogSink>>(), "ProcessingStage");
    }

    /// <summary>
    ///     Creates metrics sink for processing stage.
    /// </summary>
    private static MetricsCollectionSink CreateProcessingMetricsSink(IServiceProvider sp)
    {
        return new MetricsCollectionSink(sp.GetRequiredService<ILogger<MetricsCollectionSink>>(), "ProcessingStage");
    }

    /// <summary>
    ///     Creates alert sink for processing stage.
    /// </summary>
    private static AlertGenerationSink CreateProcessingAlertSink(IServiceProvider sp)
    {
        return new AlertGenerationSink(sp.GetRequiredService<ILogger<AlertGenerationSink>>(), "ProcessingStage");
    }

    /// <summary>
    ///     Configures services for tap node pipeline.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    public static void ConfigureServices(IServiceCollection services)
    {
        // Register pipeline nodes
        services.AddTransient<TransactionSource>();
        services.AddTransient<TransactionValidationTransform>();
        services.AddTransient<RiskAssessmentTransform>();
        services.AddTransient<ConsoleSink>();

        // Register tap sink services
        services.AddTransient<AuditLogSink>();
        services.AddTransient<MetricsCollectionSink>();
        services.AddTransient<AlertGenerationSink>();

        // Configure logging
        services.AddLogging(builder =>
        {
            builder.AddConsole();
            builder.SetMinimumLevel(LogLevel.Information);
        });
    }
}
