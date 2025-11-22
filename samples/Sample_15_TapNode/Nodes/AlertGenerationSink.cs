using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_15_TapNode.Nodes;

/// <summary>
///     Sink node that generates alerts from tapped transaction data based on configurable rules.
///     This sink is designed to be used with TapNode for non-intrusive monitoring and alerting.
/// </summary>
public sealed class AlertGenerationSink : SinkNode<Transaction>, ISinkNode<ValidatedTransaction>, ISinkNode<ProcessedTransaction>
{
    private readonly ILogger<AlertGenerationSink> _logger;
    private readonly string _pipelineStage;
    private int _alertCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AlertGenerationSink" /> class.
    /// </summary>
    /// <param name="logger">Logger for diagnostic information.</param>
    /// <param name="pipelineStage">The pipeline stage where this alert sink is placed.</param>
    public AlertGenerationSink(ILogger<AlertGenerationSink> logger, string pipelineStage)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _pipelineStage = pipelineStage ?? throw new ArgumentNullException(nameof(pipelineStage));
    }

    /// <summary>
    ///     Executes alert generation for processed transactions.
    /// </summary>
    /// <param name="input">The input data pipe.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    async Task ISinkNode<ProcessedTransaction>.ExecuteAsync(IDataPipe<ProcessedTransaction> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("AlertGenerationSink: Starting to generate alerts for processed transactions at stage {Stage}", _pipelineStage);

        try
        {
            await foreach (var processedTransaction in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await EvaluateProcessedTransactionAlertsAsync(processedTransaction, cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation("AlertGenerationSink: Completed alert generation for processed transactions at stage {Stage}. Generated {Count} alerts",
                _pipelineStage, _alertCount);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("AlertGenerationSink: Operation was cancelled at stage {Stage}", _pipelineStage);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AlertGenerationSink: Error generating alerts for processed transactions at stage {Stage}", _pipelineStage);
            throw;
        }
    }

    /// <summary>
    ///     Executes alert generation for validated transactions.
    /// </summary>
    /// <param name="input">The input data pipe.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    async Task ISinkNode<ValidatedTransaction>.ExecuteAsync(IDataPipe<ValidatedTransaction> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("AlertGenerationSink: Starting to generate alerts for validated transactions at stage {Stage}", _pipelineStage);

        try
        {
            await foreach (var validatedTransaction in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await EvaluateValidatedTransactionAlertsAsync(validatedTransaction, cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation("AlertGenerationSink: Completed alert generation for validated transactions at stage {Stage}. Generated {Count} alerts",
                _pipelineStage, _alertCount);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("AlertGenerationSink: Operation was cancelled at stage {Stage}", _pipelineStage);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AlertGenerationSink: Error generating alerts for validated transactions at stage {Stage}", _pipelineStage);
            throw;
        }
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<Transaction> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("AlertGenerationSink: Starting to generate alerts at stage {Stage}", _pipelineStage);

        try
        {
            await foreach (var transaction in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await EvaluateTransactionAlertsAsync(transaction, cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation("AlertGenerationSink: Completed alert generation at stage {Stage}. Generated {Count} alerts", _pipelineStage, _alertCount);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("AlertGenerationSink: Operation was cancelled at stage {Stage}", _pipelineStage);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AlertGenerationSink: Error generating alerts at stage {Stage}", _pipelineStage);
            throw;
        }
    }

    /// <summary>
    ///     Evaluates alerts for a transaction.
    /// </summary>
    /// <param name="transaction">The transaction to evaluate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task EvaluateTransactionAlertsAsync(Transaction transaction, CancellationToken cancellationToken)
    {
        // High-value transaction alert
        if (Math.Abs(transaction.Amount) > 2000)
        {
            await CreateAlertAsync(
                transaction,
                AlertType.HighValueTransaction,
                DetermineSeverity(Math.Abs(transaction.Amount)),
                $"High-value transaction detected: {transaction.Amount:C}",
                $"Account: {transaction.AccountNumber}, Type: {transaction.Type}",
                cancellationToken).ConfigureAwait(false);
        }

        // High-risk transaction alert
        if (transaction.RiskScore > 80)
        {
            await CreateAlertAsync(
                transaction,
                AlertType.SuspiciousPattern,
                AlertSeverity.Critical,
                $"High-risk transaction detected with score: {transaction.RiskScore}",
                $"Risk factors: Amount={transaction.Amount:C}, Flagged={transaction.IsFlagged}",
                cancellationToken).ConfigureAwait(false);
        }

        // Unusual timing alert
        if (IsUnusualTiming(transaction.Timestamp))
        {
            await CreateAlertAsync(
                transaction,
                AlertType.UnusualActivity,
                AlertSeverity.Warning,
                "Transaction at unusual time",
                $"Timestamp: {transaction.Timestamp:yyyy-MM-dd HH:mm:ss}",
                cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Evaluates alerts for a validated transaction.
    /// </summary>
    /// <param name="validatedTransaction">The validated transaction to evaluate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task EvaluateValidatedTransactionAlertsAsync(ValidatedTransaction validatedTransaction, CancellationToken cancellationToken)
    {
        var transaction = validatedTransaction.OriginalTransaction;

        // Validation failure alert
        if (!validatedTransaction.IsValid)
        {
            await CreateAlertAsync(
                transaction,
                AlertType.SuspiciousPattern,
                AlertSeverity.Error,
                "Transaction validation failed",
                $"Errors: {string.Join("; ", validatedTransaction.ValidationErrors.Take(2))}",
                cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Evaluates alerts for a processed transaction.
    /// </summary>
    /// <param name="processedTransaction">The processed transaction to evaluate.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task EvaluateProcessedTransactionAlertsAsync(ProcessedTransaction processedTransaction, CancellationToken cancellationToken)
    {
        var transaction = processedTransaction.OriginalTransaction;

        // Processing failure alert
        if (processedTransaction.Status == ProcessingStatus.Failed)
        {
            await CreateAlertAsync(
                transaction,
                AlertType.PerformanceIssue,
                AlertSeverity.Error,
                "Transaction processing failed",
                $"Notes: {processedTransaction.ProcessingNotes}",
                cancellationToken).ConfigureAwait(false);
        }

        // Manual review required alert
        if (processedTransaction.Status == ProcessingStatus.PendingReview)
        {
            await CreateAlertAsync(
                transaction,
                AlertType.SuspiciousPattern,
                AlertSeverity.Warning,
                "Transaction requires manual review",
                $"Final Risk Score: {processedTransaction.FinalRiskScore}, Duration: {processedTransaction.ProcessingDurationMs}ms",
                cancellationToken).ConfigureAwait(false);
        }

        // Performance alert for slow processing
        if (processedTransaction.ProcessingDurationMs > 1000)
        {
            await CreateAlertAsync(
                transaction,
                AlertType.PerformanceIssue,
                AlertSeverity.Warning,
                "Slow transaction processing detected",
                $"Processing time: {processedTransaction.ProcessingDurationMs}ms",
                cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Determines if transaction timestamp is unusual.
    /// </summary>
    /// <param name="timestamp">The transaction timestamp.</param>
    /// <returns>True if timing is unusual.</returns>
    private static bool IsUnusualTiming(DateTimeOffset timestamp)
    {
        var hour = timestamp.Hour;

        // Consider late night (11 PM - 5 AM) and very early morning as unusual
        return hour >= 23 || hour <= 5;
    }

    /// <summary>
    ///     Determines alert severity based on transaction amount.
    /// </summary>
    /// <param name="amount">The transaction amount.</param>
    /// <returns>The alert severity.</returns>
    private static AlertSeverity DetermineSeverity(decimal amount)
    {
        return amount switch
        {
            > 5000 => AlertSeverity.Critical,
            > 3000 => AlertSeverity.Error,
            > 2000 => AlertSeverity.Warning,
            _ => AlertSeverity.Info,
        };
    }

    /// <summary>
    ///     Creates and logs an alert.
    /// </summary>
    /// <param name="transaction">The transaction that triggered alert.</param>
    /// <param name="alertType">The type of alert.</param>
    /// <param name="severity">The alert severity.</param>
    /// <param name="description">Alert description.</param>
    /// <param name="context">Additional context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task CreateAlertAsync(
        Transaction transaction,
        AlertType alertType,
        AlertSeverity severity,
        string description,
        string context,
        CancellationToken cancellationToken)
    {
        // Simulate async alert generation (in a real system, this would send to an alerting system)
        await Task.Delay(1, cancellationToken).ConfigureAwait(false);

        var alert = new TransactionAlert
        {
            AlertId = $"ALT{++_alertCount:D6}",
            Transaction = transaction,
            AlertType = alertType,
            Severity = severity,
            AlertTimestamp = DateTimeOffset.UtcNow,
            Description = description,
            Context = context,
        };

        await LogAlertAsync(alert, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Logs an alert to alerting system.
    /// </summary>
    /// <param name="alert">The alert to log.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task LogAlertAsync(TransactionAlert alert, CancellationToken cancellationToken)
    {
        // Simulate async alert logging
        await Task.Delay(1, cancellationToken).ConfigureAwait(false);

        _logger.LogWarning(
            "ALERT: {AlertId} - {Severity} - {Type} - {TransactionId} - {Description} - {Context}",
            alert.AlertId,
            alert.Severity,
            alert.AlertType,
            alert.Transaction.TransactionId,
            alert.Description,
            alert.Context ?? string.Empty);

        // In a real implementation, you would:
        // - Send alerts to PagerDuty, OpsGenie, or similar systems
        // - Create tickets in issue tracking systems
        // - Send email/SMS notifications
        // - Integrate with monitoring dashboards
        // - Store alerts in a dedicated alert database
        // - Implement alert escalation policies
    }
}
