# TapNode Sample

This sample demonstrates NPipeline's `TapNode<T>` functionality for non-intrusive monitoring, audit logging, metrics collection, and alert generation without
affecting the main data processing flow.

## Overview

The sample showcases a realistic financial transaction processing pipeline with comprehensive monitoring capabilities using TapNode at multiple stages. It
demonstrates how to add observability, compliance, and operational intelligence to data pipelines without modifying core business logic.

## Key Concepts Demonstrated

### TapNode<T> Fundamentals

- **Non-Intrusive Monitoring**: Tap into data streams without affecting the main processing flow
- **Multiple Tap Points**: Monitor at different stages of the pipeline
- **Side Effects**: Create audit trails, metrics, and alerts as side effects
- **Error Isolation**: Tap failures don't impact the main pipeline
- **Performance Monitoring**: Track processing times and identify bottlenecks

### Real-World Scenarios

- **Financial Transaction Processing**: Realistic transaction validation and risk assessment
- **Audit Compliance**: Comprehensive audit trail generation for regulatory requirements
- **Operational Monitoring**: Real-time metrics collection and performance tracking
- **Risk Management**: Automated alert generation for suspicious activities
- **Business Intelligence**: Transaction pattern analysis and reporting

## Pipeline Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  Transaction    │    │   Transaction    │    │   Risk          │    │   Processed     │
│  Source         │───▶│  Validation      │───▶│  Assessment      │───▶│  Transactions   │
│                 │    │                  │    │                  │    │  (Console Sink) │
└─────────┬───────┘    └─────────┬────────┘    └─────────┬────────┘    └─────────────────┘
          │                      │                      │
          ▼                      ▼                      ▼
    ┌─────────┐           ┌─────────────┐        ┌─────────────┐
    │ Tap:    │           │ Tap:        │        │ Tap:        │
    │ Audit   │           │ Audit       │        │ Audit       │
    │ Metrics  │           │ Metrics     │        │ Metrics     │
    │ Alerts  │           │ Alerts      │        │ Alerts      │
    └─────────┘           └─────────────┘        └─────────────┘
```

*Figure: TapNode pattern showing non-intrusive monitoring at multiple pipeline stages*

## Data Models

### Core Entities

- **Transaction**: Financial transaction data with validation and risk information
- **ValidatedTransaction**: Transaction with validation results and status
- **ProcessedTransaction**: Final processed transaction with risk assessment
- **AuditLogEntry**: Audit trail entries for compliance and debugging
- **TransactionMetrics**: Aggregated metrics for monitoring and reporting
- **TransactionAlert**: Real-time alerts for suspicious activities

### Key Features

- **Comprehensive Validation**: Business rule validation with detailed error reporting
- **Risk Assessment**: Multi-factor risk scoring with configurable thresholds
- **Audit Trail**: Complete transaction lifecycle tracking
- **Performance Metrics**: Real-time processing statistics and KPIs
- **Alert System**: Configurable alerts for various scenarios

## Pipeline Components

### Main Processing Flow

1. **TransactionSource**: Generates realistic transaction data with various characteristics
2. **TransactionValidationTransform**: Validates business rules and data integrity
3. **RiskAssessmentTransform**: Performs comprehensive risk analysis
4. **ConsoleSink**: Displays final processing results

### TapNode Monitoring Points

1. **Source Stage Taps**:
    - **Audit Tap**: Logs all incoming transactions
    - **Metrics Tap**: Collects input statistics
    - **Alert Tap**: Generates alerts for high-value or suspicious transactions

2. **Validation Stage Taps**:
    - **Audit Tap**: Tracks validation results and failures
    - **Metrics Tap**: Monitors validation performance
    - **Alert Tap**: Alerts on validation failures and patterns

3. **Processing Stage Taps**:
    - **Audit Tap**: Records final processing outcomes
    - **Metrics Tap**: Tracks overall pipeline performance
    - **Alert Tap**: Monitors processing issues and delays

## Running the Sample

### Prerequisites

- .NET 8.0 or later
- NPipeline framework

### Execution

```bash
dotnet run --project Sample_15_TapNode.csproj
```

### Expected Output

The sample produces three types of output:

1. **Main Processing Results**: Transaction processing outcomes with status and timing
2. **Audit Logs**: Detailed audit trail showing transaction flow through pipeline stages
3. **Metrics Reports**: Statistical summaries for each pipeline stage
4. **Alert Notifications**: Real-time alerts for suspicious activities and performance issues

### Sample Output

```
=== TRANSACTION PROCESSING RESULTS ===

✅ [Approved     ] ID: TXN000001 | Account: ACC001 | Amount:    $125.50 | Type: Payment    | Risk:  25 | Time:   45ms
✅ [Approved     ] ID: TXN000002 | Account: ACC002 | Amount:    $250.00 | Type: Deposit    | Risk:  15 | Time:   38ms
⏳ [PendingReview] ID: TXN000003 | Account: ACC003 | Amount:  $1,500.00 | Type: Transfer    | Risk:  85 | Time:  125ms
  Details: High-risk transaction detected with score: 85

AUDIT: AUD000001 - TransactionReceived - TXN000001 - SourceStage - Transaction TXN000001 received. Amount: $125.50, Type: Payment
METRICS: SourceStage - Total: 50, Amount: $12,450.75, Avg: $249.02, High Risk: 5, Flagged: 8, Avg Risk: 42.3
ALERT: ALT000001 - Warning - HighValueTransaction - TXN000003 - High-value transaction detected: $1,500.00

=== PROCESSING SUMMARY ===
Total Processed: 50
✅ Approved: 42 (84.0%)
❌ Rejected: 3 (6.0%)
⏳ Pending Review: 4 (8.0%)
💥 Failed: 1 (2.0%)
```

## TapNode Benefits Demonstrated

### 1. Non-Intrusive Monitoring

```csharp
// Main processing continues uninterrupted
var tapNode = new TapNode<Transaction>(auditSink);
// Transaction flows through main pipeline while audit happens in parallel
```

### 2. Multiple Monitoring Points

```csharp
// Monitor at different stages without code duplication
builder.AddTap("sourceTap", sp => new TapNode<Transaction>(sourceAuditSink));
builder.AddTap("validationTap", sp => new TapNode<ValidatedTransaction>(validationAuditSink));
builder.AddTap("processingTap", sp => new TapNode<ProcessedTransaction>(processingAuditSink));
```

### 3. Side Effects Without Core Logic Changes

```csharp
// Add monitoring without modifying business logic
public class AuditLogSink : SinkNode<Transaction>
{
    // Audit logic completely separate from main processing
}
```

### 4. Error Isolation

```csharp
// Tap failures don't affect main pipeline
// Even if audit sink fails, transactions continue processing
```

### 5. Performance Monitoring

```csharp
// Track processing times and identify bottlenecks
public class MetricsCollectionSink : SinkNode<Transaction>
{
    // Collect performance metrics without impacting main flow
}
```

## Performance Considerations

### Memory Usage

- **Tap Buffering**: Each tap maintains separate processing queues
- **Data Duplication**: Consider memory impact for high-throughput streams
- **Cleanup Strategy**: Implement appropriate cleanup for long-running pipelines

### Throughput Impact

- **Async Processing**: Ensure tap sinks use async operations
- **Backpressure Management**: Handle slow tap sinks appropriately
- **Resource Allocation**: Balance monitoring overhead with processing requirements

### Optimization Tips

```csharp
// Use efficient data structures for metrics
private readonly ConcurrentDictionary<TransactionType, long> _transactionCounts = new();

// Implement batching for audit logs
await FlushAuditBatchAsync(auditEntries);

// Use sampling for high-frequency metrics
if (_processedCount % 100 == 0) CollectMetrics();
```

## Real-World Applications

### Financial Services

- **Regulatory Compliance**: Complete audit trails for financial transactions
- **Fraud Detection**: Real-time monitoring and alerting for suspicious activities
- **Risk Management**: Continuous risk assessment and reporting
- **Performance Monitoring**: SLA tracking and bottleneck identification

### E-commerce

- **Order Processing**: Monitor order flow and conversion metrics
- **Inventory Management**: Track stock levels and reorder alerts
- **Customer Behavior**: Analyze shopping patterns and preferences
- **Fraud Prevention**: Detect and prevent fraudulent transactions

### IoT and Manufacturing

- **Sensor Data**: Monitor equipment health and performance
- **Quality Control**: Track defect rates and production metrics
- **Predictive Maintenance**: Alert on equipment failure risks
- **Operational Efficiency**: Monitor throughput and utilization

### Healthcare

- **Patient Monitoring**: Track vital signs and alert on anomalies
- **Medication Tracking**: Audit medication administration and compliance
- **Clinical Trials**: Monitor trial data and protocol adherence
- **Regulatory Compliance**: Maintain audit trails for medical records

## Advanced Configuration

### Custom Tap Implementations

```csharp
public class CustomTapNode<T> : TapNode<T>
{
    public CustomTapNode(ISinkNode<T> sink, ICustomLogic logic) : base(sink)
    {
        _logic = logic;
    }

    public override async Task<T> ExecuteAsync(T item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Custom tap logic before sending to sink
        await _logic.ProcessAsync(item, cancellationToken);
        return await base.ExecuteAsync(item, context, cancellationToken);
    }
}
```

### Conditional Tapping

```csharp
public class ConditionalTapNode<T> : TapNode<T>
{
    private readonly Func<T, bool> _condition;

    protected override async Task SendToSinkAsync(T item, CancellationToken cancellationToken)
    {
        if (_condition(item))
        {
            await base.SendToSinkAsync(item, cancellationToken);
        }
    }
}
```

### Tap Chaining

```csharp
// Chain multiple taps for complex monitoring scenarios
var auditTap = new TapNode<Transaction>(auditSink);
var metricsTap = new TapNode<Transaction>(metricsSink);
var alertTap = new TapNode<Transaction>(alertSink);
```

## Testing

The sample includes comprehensive tests using NPipeline.Extensions.Testing:

### Test Scenarios

- **Basic Processing**: Verify correct transaction processing
- **High-Risk Handling**: Test alert generation for suspicious transactions
- **Error Handling**: Validate behavior with invalid transactions
- **Multiple Tap Points**: Ensure independent operation of multiple taps
- **Error Isolation**: Verify tap failures don't affect main pipeline
- **Volume Testing**: Test performance with large transaction volumes
- **Metrics Accuracy**: Validate metrics collection and aggregation

### Running Tests

```bash
dotnet test Sample_15_TapNode.csproj
```

## Best Practices

### 1. Pipeline Design

- **Separate Concerns**: Keep monitoring logic separate from business logic
- **Minimal Overhead**: Optimize tap implementations for performance
- **Error Handling**: Implement robust error handling in tap sinks
- **Resource Management**: Properly dispose of tap resources

### 2. Monitoring Strategy

- **Relevant Metrics**: Collect metrics that provide actionable insights
- **Appropriate Alerting**: Avoid alert fatigue with meaningful thresholds
- **Audit Completeness**: Ensure comprehensive audit trails for compliance
- **Performance Impact**: Monitor the impact of monitoring on main pipeline

### 3. Operational Considerations

- **Log Retention**: Implement appropriate log retention policies
- **Metrics Storage**: Choose suitable storage for time-series data
- **Alert Escalation**: Define clear escalation procedures
- **Documentation**: Document monitoring configurations and procedures

## Extending the Sample

### Adding New Tap Types

```csharp
public class ComplianceTapSink : SinkNode<Transaction>
{
    // Implement compliance-specific monitoring
}

// Register in pipeline
builder.AddTap("complianceTap", sp => new TapNode<Transaction>(complianceSink));
```

### Custom Metrics

```csharp
public class BusinessMetricsSink : SinkNode<ProcessedTransaction>
{
    // Implement business-specific metrics
}
```

### Integration with External Systems

```csharp
public class ExternalAlertSink : SinkNode<TransactionAlert>
{
    private readonly IAlertService _alertService;

    public ExternalAlertSink(IAlertService alertService)
    {
        _alertService = alertService;
    }

    // Send alerts to external monitoring systems
}
```

## Troubleshooting

### Common Issues

1. **Tap Not Receiving Data**
    - Verify pipeline connections
    - Check tap node registration
    - Ensure proper data flow configuration

2. **Performance Degradation**
    - Optimize tap sink implementations
    - Implement batching for high-volume scenarios
    - Consider sampling for metrics collection

3. **Memory Issues**
    - Implement proper cleanup in tap sinks
    - Use streaming instead of buffering where possible
    - Monitor memory usage in production

4. **Alert Fatigue**
    - Adjust alert thresholds
    - Implement alert aggregation
    - Use severity levels appropriately

### Debugging Tips

- Enable debug logging for tap nodes
- Monitor tap sink performance metrics
- Test with isolated tap implementations
- Verify error handling in tap sinks

## Conclusion

This sample demonstrates production-ready patterns for using TapNode in NPipeline applications. The combination of non-intrusive monitoring, comprehensive audit
trails, real-time alerting, and performance metrics provides a foundation for building robust, observable, and compliant data processing systems.

TapNode enables organizations to add sophisticated monitoring capabilities without modifying core business logic, making it an essential tool for enterprise
data processing applications.

For more information on NPipeline's advanced features, see the [NPipeline Documentation](../../docs/README.md).
