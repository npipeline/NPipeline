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
///     Sink node that creates audit log entries from tapped transaction data.
///     This sink is designed to be used with TapNode to create non-intrusive audit trails.
/// </summary>
public sealed class AuditLogSink : SinkNode<Transaction>, ISinkNode<ValidatedTransaction>, ISinkNode<ProcessedTransaction>
{
    private readonly ILogger<AuditLogSink> _logger;
    private readonly string _pipelineStage;
    private int _auditCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AuditLogSink" /> class.
    /// </summary>
    /// <param name="logger">Logger for diagnostic information.</param>
    /// <param name="pipelineStage">The pipeline stage where this audit sink is placed.</param>
    public AuditLogSink(ILogger<AuditLogSink> logger, string pipelineStage)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _pipelineStage = pipelineStage ?? throw new ArgumentNullException(nameof(pipelineStage));
    }

    /// <summary>
    ///     Executes the audit sink for processed transactions.
    /// </summary>
    /// <param name="input">The input data pipe.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    async Task ISinkNode<ProcessedTransaction>.ExecuteAsync(IDataPipe<ProcessedTransaction> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("AuditLogSink: Starting to audit processed transactions at stage {Stage}", _pipelineStage);

        try
        {
            await foreach (var processedTransaction in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var eventType = processedTransaction.Status switch
                {
                    ProcessingStatus.Approved => AuditEventType.ProcessingCompleted,
                    ProcessingStatus.PendingReview => AuditEventType.TransactionFlagged,
                    ProcessingStatus.Rejected => AuditEventType.TransactionFlagged,
                    ProcessingStatus.Failed => AuditEventType.TransactionFlagged,
                    _ => AuditEventType.ProcessingCompleted,
                };

                await AuditProcessedTransactionAsync(processedTransaction, eventType, cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation("AuditLogSink: Completed auditing {Count} processed transactions at stage {Stage}", _auditCount, _pipelineStage);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("AuditLogSink: Operation was cancelled at stage {Stage}", _pipelineStage);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AuditLogSink: Error auditing processed transactions at stage {Stage}", _pipelineStage);
            throw;
        }
    }

    /// <summary>
    ///     Executes the audit sink for validated transactions.
    /// </summary>
    /// <param name="input">The input data pipe.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    async Task ISinkNode<ValidatedTransaction>.ExecuteAsync(IDataPipe<ValidatedTransaction> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("AuditLogSink: Starting to audit validated transactions at stage {Stage}", _pipelineStage);

        try
        {
            await foreach (var validatedTransaction in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var eventType = validatedTransaction.IsValid
                    ? AuditEventType.ValidationCompleted
                    : AuditEventType.TransactionFlagged;

                await AuditValidatedTransactionAsync(validatedTransaction, eventType, cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation("AuditLogSink: Completed auditing {Count} validated transactions at stage {Stage}", _auditCount, _pipelineStage);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("AuditLogSink: Operation was cancelled at stage {Stage}", _pipelineStage);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AuditLogSink: Error auditing validated transactions at stage {Stage}", _pipelineStage);
            throw;
        }
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<Transaction> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("AuditLogSink: Starting to audit transactions at stage {Stage}", _pipelineStage);

        try
        {
            await foreach (var transaction in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                await AuditTransactionAsync(transaction, AuditEventType.TransactionReceived, cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation("AuditLogSink: Completed auditing {Count} transactions at stage {Stage}", _auditCount, _pipelineStage);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("AuditLogSink: Operation was cancelled at stage {Stage}", _pipelineStage);
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AuditLogSink: Error auditing transactions at stage {Stage}", _pipelineStage);
            throw;
        }
    }

    /// <summary>
    ///     Creates an audit log entry for a transaction.
    /// </summary>
    /// <param name="transaction">The transaction to audit.</param>
    /// <param name="eventType">The type of audit event.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task AuditTransactionAsync(Transaction transaction, AuditEventType eventType, CancellationToken cancellationToken)
    {
        var auditEntry = new AuditLogEntry
        {
            AuditId = $"AUD{++_auditCount:D6}",
            Transaction = transaction,
            EventType = eventType,
            AuditTimestamp = DateTimeOffset.UtcNow,
            PipelineStage = _pipelineStage,
            Details = GenerateTransactionDetails(transaction, eventType),
        };

        await LogAuditEntryAsync(auditEntry, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Creates an audit log entry for a validated transaction.
    /// </summary>
    /// <param name="validatedTransaction">The validated transaction to audit.</param>
    /// <param name="eventType">The type of audit event.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task AuditValidatedTransactionAsync(ValidatedTransaction validatedTransaction, AuditEventType eventType, CancellationToken cancellationToken)
    {
        var auditEntry = new AuditLogEntry
        {
            AuditId = $"AUD{++_auditCount:D6}",
            Transaction = validatedTransaction.OriginalTransaction,
            EventType = eventType,
            AuditTimestamp = DateTimeOffset.UtcNow,
            PipelineStage = _pipelineStage,
            Details = GenerateValidatedTransactionDetails(validatedTransaction, eventType),
        };

        await LogAuditEntryAsync(auditEntry, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Creates an audit log entry for a processed transaction.
    /// </summary>
    /// <param name="processedTransaction">The processed transaction to audit.</param>
    /// <param name="eventType">The type of audit event.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task AuditProcessedTransactionAsync(ProcessedTransaction processedTransaction, AuditEventType eventType, CancellationToken cancellationToken)
    {
        var auditEntry = new AuditLogEntry
        {
            AuditId = $"AUD{++_auditCount:D6}",
            Transaction = processedTransaction.OriginalTransaction,
            EventType = eventType,
            AuditTimestamp = DateTimeOffset.UtcNow,
            PipelineStage = _pipelineStage,
            Details = GenerateProcessedTransactionDetails(processedTransaction, eventType),
        };

        await LogAuditEntryAsync(auditEntry, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Generates audit details for a transaction.
    /// </summary>
    /// <param name="transaction">The transaction.</param>
    /// <param name="eventType">The audit event type.</param>
    /// <returns>Audit details string.</returns>
    private string GenerateTransactionDetails(Transaction transaction, AuditEventType eventType)
    {
        return eventType switch
        {
            AuditEventType.TransactionReceived => $"Transaction {transaction.TransactionId} received. Amount: {transaction.Amount:C}, Type: {transaction.Type}",
            AuditEventType.HighRiskTransaction => $"High-risk transaction {transaction.TransactionId} detected. Risk Score: {transaction.RiskScore}",
            _ => $"Transaction {transaction.TransactionId} audited. Event: {eventType}",
        };
    }

    /// <summary>
    ///     Generates audit details for a validated transaction.
    /// </summary>
    /// <param name="validatedTransaction">The validated transaction.</param>
    /// <param name="eventType">The audit event type.</param>
    /// <returns>Audit details string.</returns>
    private string GenerateValidatedTransactionDetails(ValidatedTransaction validatedTransaction, AuditEventType eventType)
    {
        var transaction = validatedTransaction.OriginalTransaction;

        if (validatedTransaction.IsValid)
            return $"Transaction {transaction.TransactionId} validation passed. Status: {validatedTransaction.ProcessingStatus}";

        var errors = string.Join("; ", validatedTransaction.ValidationErrors.Take(3));
        return $"Transaction {transaction.TransactionId} validation failed. Errors: {errors}";
    }

    /// <summary>
    ///     Generates audit details for a processed transaction.
    /// </summary>
    /// <param name="processedTransaction">The processed transaction.</param>
    /// <param name="eventType">The audit event type.</param>
    /// <returns>Audit details string.</returns>
    private string GenerateProcessedTransactionDetails(ProcessedTransaction processedTransaction, AuditEventType eventType)
    {
        var transaction = processedTransaction.OriginalTransaction;

        var details = $"Transaction {transaction.TransactionId} processed. " +
                      $"Status: {processedTransaction.Status}, " +
                      $"Final Risk Score: {processedTransaction.FinalRiskScore}, " +
                      $"Duration: {processedTransaction.ProcessingDurationMs}ms";

        if (!string.IsNullOrEmpty(processedTransaction.ProcessingNotes))
            details += $", Notes: {processedTransaction.ProcessingNotes}";

        return details;
    }

    /// <summary>
    ///     Logs an audit entry to the audit trail.
    /// </summary>
    /// <param name="auditEntry">The audit entry to log.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task LogAuditEntryAsync(AuditLogEntry auditEntry, CancellationToken cancellationToken)
    {
        // Simulate async audit logging (in a real system, this would write to a database or file)
        await Task.Delay(1, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "AUDIT: {AuditId} - {EventType} - {TransactionId} - {Stage} - {Details}",
            auditEntry.AuditId,
            auditEntry.EventType,
            auditEntry.Transaction.TransactionId,
            auditEntry.PipelineStage,
            auditEntry.Details);

        // In a real implementation, you would:
        // - Write to a dedicated audit database table
        // - Send to a log aggregation system
        // - Store in immutable storage for compliance
        // - Include additional metadata like user ID, IP address, etc.
    }
}
