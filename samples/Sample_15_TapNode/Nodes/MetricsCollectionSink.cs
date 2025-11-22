using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_15_TapNode.Nodes;

/// <summary>
///     Sink node that collects and aggregates transaction metrics from tapped data.
///     This sink is designed to be used with TapNode for non-intrusive monitoring.
/// </summary>
public sealed class MetricsCollectionSink : SinkNode<Transaction>, ISinkNode<ValidatedTransaction>, ISinkNode<ProcessedTransaction>
{
    private readonly MetricsAggregator _aggregator;
    private readonly ILogger<MetricsCollectionSink> _logger;
    private readonly string _pipelineStage;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MetricsCollectionSink" /> class.
    /// </summary>
    /// <param name="logger">Logger for diagnostic information.</param>
    /// <param name="pipelineStage">The pipeline stage where this metrics sink is placed.</param>
    public MetricsCollectionSink(ILogger<MetricsCollectionSink> logger, string pipelineStage)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _pipelineStage = pipelineStage ?? throw new ArgumentNullException(nameof(pipelineStage));
        _aggregator = new MetricsAggregator();
    }

    /// <summary>
    ///     Executes the metrics collection for processed transactions.
    /// </summary>
    /// <param name="input">The input data pipe.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    async Task ISinkNode<ProcessedTransaction>.ExecuteAsync(IDataPipe<ProcessedTransaction> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("MetricsCollectionSink: Starting to collect metrics for processed transactions at stage {Stage}", _pipelineStage);

        try
        {
            await foreach (var processedTransaction in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                _aggregator.AddProcessedTransaction(processedTransaction);
            }

            await ReportMetricsAsync(cancellationToken).ConfigureAwait(false);

            _logger.LogInformation("MetricsCollectionSink: Completed metrics collection for processed transactions at stage {Stage}", _pipelineStage);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("MetricsCollectionSink: Operation was cancelled at stage {Stage}", _pipelineStage);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MetricsCollectionSink: Error collecting metrics for processed transactions at stage {Stage}", _pipelineStage);
            throw;
        }
    }

    /// <summary>
    ///     Executes the metrics collection for validated transactions.
    /// </summary>
    /// <param name="input">The input data pipe.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    async Task ISinkNode<ValidatedTransaction>.ExecuteAsync(IDataPipe<ValidatedTransaction> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("MetricsCollectionSink: Starting to collect metrics for validated transactions at stage {Stage}", _pipelineStage);

        try
        {
            await foreach (var validatedTransaction in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                _aggregator.AddValidatedTransaction(validatedTransaction);
            }

            await ReportMetricsAsync(cancellationToken).ConfigureAwait(false);

            _logger.LogInformation("MetricsCollectionSink: Completed metrics collection for validated transactions at stage {Stage}", _pipelineStage);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("MetricsCollectionSink: Operation was cancelled at stage {Stage}", _pipelineStage);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MetricsCollectionSink: Error collecting metrics for validated transactions at stage {Stage}", _pipelineStage);
            throw;
        }
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<Transaction> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("MetricsCollectionSink: Starting to collect metrics at stage {Stage}", _pipelineStage);

        try
        {
            await foreach (var transaction in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                _aggregator.AddTransaction(transaction);
            }

            await ReportMetricsAsync(cancellationToken).ConfigureAwait(false);

            _logger.LogInformation("MetricsCollectionSink: Completed metrics collection at stage {Stage}", _pipelineStage);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("MetricsCollectionSink: Operation was cancelled at stage {Stage}", _pipelineStage);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MetricsCollectionSink: Error collecting metrics at stage {Stage}", _pipelineStage);
            throw;
        }
    }

    /// <summary>
    ///     Reports the collected metrics.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task ReportMetricsAsync(CancellationToken cancellationToken)
    {
        var metrics = _aggregator.GetMetrics(_pipelineStage);

        // Simulate async metrics reporting (in a real system, this would send to a monitoring system)
        await Task.Delay(1, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "METRICS: {Stage} - Total: {Total}, Amount: {TotalAmount:C}, Avg: {AvgAmount:C}, High Risk: {HighRisk}, Flagged: {Flagged}, Avg Risk: {AvgRisk:F1}",
            metrics.PipelineStage,
            metrics.TotalTransactions,
            metrics.TotalAmount,
            metrics.AverageAmount,
            metrics.HighRiskCount,
            metrics.FlaggedCount,
            metrics.AverageRiskScore);

        // Log transaction type breakdown
        foreach (var kvp in metrics.TransactionCounts.OrderByDescending(x => x.Value))
        {
            _logger.LogInformation(
                "METRICS: {Stage} - {Type}: {Count} transactions",
                metrics.PipelineStage,
                kvp.Key,
                kvp.Value);
        }

        // In a real implementation, you would:
        // - Send metrics to Prometheus, Grafana, or other monitoring systems
        // - Create custom dashboards and alerts
        // - Store metrics in a time-series database
        // - Generate performance reports
    }

    /// <summary>
    ///     Aggregates transaction metrics.
    /// </summary>
    private sealed class MetricsAggregator
    {
        private readonly Dictionary<ProcessingStatus, long> _processingStatusCounts = new();
        private readonly Dictionary<TransactionType, long> _transactionCounts = new();
        private long _flaggedCount;
        private long _highRiskCount;
        private long _processedTransactions;
        private decimal _totalAmount;
        private double _totalRiskScore;
        private long _totalTransactions;
        private long _validTransactions;

        /// <summary>
        ///     Adds a transaction to the metrics.
        /// </summary>
        /// <param name="transaction">The transaction to add.</param>
        public void AddTransaction(Transaction transaction)
        {
            _totalTransactions++;
            _totalAmount += transaction.Amount;

            _transactionCounts[transaction.Type] = _transactionCounts.GetValueOrDefault(transaction.Type) + 1;

            if (transaction.RiskScore > 70)
                _highRiskCount++;

            if (transaction.IsFlagged)
                _flaggedCount++;

            _totalRiskScore += transaction.RiskScore;
        }

        /// <summary>
        ///     Adds a validated transaction to the metrics.
        /// </summary>
        /// <param name="validatedTransaction">The validated transaction to add.</param>
        public void AddValidatedTransaction(ValidatedTransaction validatedTransaction)
        {
            AddTransaction(validatedTransaction.OriginalTransaction);

            if (validatedTransaction.IsValid)
                _validTransactions++;
        }

        /// <summary>
        ///     Adds a processed transaction to the metrics.
        /// </summary>
        /// <param name="processedTransaction">The processed transaction to add.</param>
        public void AddProcessedTransaction(ProcessedTransaction processedTransaction)
        {
            AddTransaction(processedTransaction.OriginalTransaction);

            _processedTransactions++;

            _processingStatusCounts[processedTransaction.Status] =
                _processingStatusCounts.GetValueOrDefault(processedTransaction.Status) + 1;
        }

        /// <summary>
        ///     Gets the current metrics.
        /// </summary>
        /// <param name="pipelineStage">The pipeline stage.</param>
        /// <returns>Transaction metrics.</returns>
        public TransactionMetrics GetMetrics(string pipelineStage)
        {
            return new TransactionMetrics
            {
                MetricsTimestamp = DateTimeOffset.UtcNow,
                TotalTransactions = _totalTransactions,
                TotalAmount = _totalAmount,
                TransactionCounts = new Dictionary<TransactionType, long>(_transactionCounts),
                AverageAmount = _totalTransactions > 0
                    ? _totalAmount / _totalTransactions
                    : 0,
                HighRiskCount = _highRiskCount,
                FlaggedCount = _flaggedCount,
                AverageRiskScore = _totalTransactions > 0
                    ? _totalRiskScore / _totalTransactions
                    : 0,
                PipelineStage = pipelineStage,
            };
        }
    }
}
