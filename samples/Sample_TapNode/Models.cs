using System;
using System.Collections.Generic;

namespace Sample_TapNode;

/// <summary>
///     Represents a financial transaction that will be processed through the main pipeline.
/// </summary>
public sealed record Transaction
{
    /// <summary>
    ///     Unique identifier for the transaction.
    /// </summary>
    public required string TransactionId { get; init; }

    /// <summary>
    ///     Account number associated with the transaction.
    /// </summary>
    public required string AccountNumber { get; init; }

    /// <summary>
    ///     Transaction amount (positive for credits, negative for debits).
    /// </summary>
    public required decimal Amount { get; init; }

    /// <summary>
    ///     Type of transaction (Deposit, Withdrawal, Transfer, Payment).
    /// </summary>
    public required TransactionType Type { get; init; }

    /// <summary>
    ///     Timestamp when the transaction occurred.
    /// </summary>
    public required DateTimeOffset Timestamp { get; init; }

    /// <summary>
    ///     Description of the transaction.
    /// </summary>
    public required string Description { get; init; }

    /// <summary>
    ///     Merchant or recipient information.
    /// </summary>
    public string? Merchant { get; init; }

    /// <summary>
    ///     Category of the transaction (e.g., Food, Transportation, Entertainment).
    /// </summary>
    public string? Category { get; init; }

    /// <summary>
    ///     Whether the transaction is flagged for review.
    /// </summary>
    public bool IsFlagged { get; init; }

    /// <summary>
    ///     Risk score calculated for the transaction (0-100).
    /// </summary>
    public int RiskScore { get; init; }
}

/// <summary>
///     Types of transactions supported by the system.
/// </summary>
public enum TransactionType
{
    /// <summary>
    ///     Money deposited into an account.
    /// </summary>
    Deposit,

    /// <summary>
    ///     Money withdrawn from an account.
    /// </summary>
    Withdrawal,

    /// <summary>
    ///     Money transferred between accounts.
    /// </summary>
    Transfer,

    /// <summary>
    ///     Payment made to a merchant or service.
    /// </summary>
    Payment,
}

/// <summary>
///     Represents a processed transaction with additional processing information.
/// </summary>
public sealed record ProcessedTransaction
{
    /// <summary>
    ///     Original transaction data.
    /// </summary>
    public required Transaction OriginalTransaction { get; init; }

    /// <summary>
    ///     Processing status of the transaction.
    /// </summary>
    public required ProcessingStatus Status { get; init; }

    /// <summary>
    ///     Timestamp when the transaction was processed.
    /// </summary>
    public required DateTimeOffset ProcessedAt { get; init; }

    /// <summary>
    ///     Processing duration in milliseconds.
    /// </summary>
    public required long ProcessingDurationMs { get; init; }

    /// <summary>
    ///     Additional processing notes or error messages.
    /// </summary>
    public string? ProcessingNotes { get; init; }

    /// <summary>
    ///     Updated risk score after processing.
    /// </summary>
    public required int FinalRiskScore { get; init; }
}

/// <summary>
///     Processing status of a transaction.
/// </summary>
public enum ProcessingStatus
{
    /// <summary>
    ///     Transaction was processed successfully.
    /// </summary>
    Approved,

    /// <summary>
    ///     Transaction was rejected due to validation or risk rules.
    /// </summary>
    Rejected,

    /// <summary>
    ///     Transaction requires manual review.
    /// </summary>
    PendingReview,

    /// <summary>
    ///     Transaction failed to process due to system error.
    /// </summary>
    Failed,
}

/// <summary>
///     Represents an audit log entry created by tapping into the transaction stream.
/// </summary>
public sealed record AuditLogEntry
{
    /// <summary>
    ///     Unique identifier for the audit entry.
    /// </summary>
    public required string AuditId { get; init; }

    /// <summary>
    ///     Transaction being audited.
    /// </summary>
    public required Transaction Transaction { get; init; }

    /// <summary>
    ///     Type of audit event.
    /// </summary>
    public required AuditEventType EventType { get; init; }

    /// <summary>
    ///     Timestamp when the audit event was created.
    /// </summary>
    public required DateTimeOffset AuditTimestamp { get; init; }

    /// <summary>
    ///     Pipeline stage where the audit occurred.
    /// </summary>
    public required string PipelineStage { get; init; }

    /// <summary>
    ///     Additional audit data.
    /// </summary>
    public string? Details { get; init; }
}

/// <summary>
///     Types of audit events that can be captured.
/// </summary>
public enum AuditEventType
{
    /// <summary>
    ///     Transaction received by the system.
    /// </summary>
    TransactionReceived,

    /// <summary>
    ///     Transaction validation completed.
    /// </summary>
    ValidationCompleted,

    /// <summary>
    ///     Risk assessment completed.
    /// </summary>
    RiskAssessmentCompleted,

    /// <summary>
    ///     Transaction processing completed.
    /// </summary>
    ProcessingCompleted,

    /// <summary>
    ///     Transaction was flagged for review.
    /// </summary>
    TransactionFlagged,

    /// <summary>
    ///     High-risk transaction detected.
    /// </summary>
    HighRiskTransaction,
}

/// <summary>
///     Represents metrics collected from the transaction stream for monitoring purposes.
/// </summary>
public sealed record TransactionMetrics
{
    /// <summary>
    ///     Timestamp when the metrics were collected.
    /// </summary>
    public required DateTimeOffset MetricsTimestamp { get; init; }

    /// <summary>
    ///     Total number of transactions processed.
    /// </summary>
    public required long TotalTransactions { get; init; }

    /// <summary>
    ///     Total transaction amount processed.
    /// </summary>
    public required decimal TotalAmount { get; init; }

    /// <summary>
    ///     Number of transactions by type.
    /// </summary>
    public required Dictionary<TransactionType, long> TransactionCounts { get; init; }

    /// <summary>
    ///     Average transaction amount.
    /// </summary>
    public required decimal AverageAmount { get; init; }

    /// <summary>
    ///     Number of high-risk transactions (risk score > 70).
    /// </summary>
    public required long HighRiskCount { get; init; }

    /// <summary>
    ///     Number of flagged transactions.
    /// </summary>
    public required long FlaggedCount { get; init; }

    /// <summary>
    ///     Average risk score across all transactions.
    /// </summary>
    public required double AverageRiskScore { get; init; }

    /// <summary>
    ///     Processing stage where metrics were collected.
    /// </summary>
    public required string PipelineStage { get; init; }
}

/// <summary>
///     Represents an alert generated from monitoring the transaction stream.
/// </summary>
public sealed record TransactionAlert
{
    /// <summary>
    ///     Unique identifier for the alert.
    /// </summary>
    public required string AlertId { get; init; }

    /// <summary>
    ///     Transaction that triggered the alert.
    /// </summary>
    public required Transaction Transaction { get; init; }

    /// <summary>
    ///     Type of alert.
    /// </summary>
    public required AlertType AlertType { get; init; }

    /// <summary>
    ///     Severity level of the alert.
    /// </summary>
    public required AlertSeverity Severity { get; init; }

    /// <summary>
    ///     Timestamp when the alert was generated.
    /// </summary>
    public required DateTimeOffset AlertTimestamp { get; init; }

    /// <summary>
    ///     Description of the alert.
    /// </summary>
    public required string Description { get; init; }

    /// <summary>
    ///     Additional alert context.
    /// </summary>
    public string? Context { get; init; }
}

/// <summary>
///     Types of alerts that can be generated.
/// </summary>
public enum AlertType
{
    /// <summary>
    ///     High-value transaction detected.
    /// </summary>
    HighValueTransaction,

    /// <summary>
    ///     Suspicious transaction pattern detected.
    /// </summary>
    SuspiciousPattern,

    /// <summary>
    ///     Unusual transaction for this account.
    /// </summary>
    UnusualActivity,

    /// <summary>
    ///     Transaction limit exceeded.
    /// </summary>
    LimitExceeded,

    /// <summary>
    ///     Processing performance issue detected.
    /// </summary>
    PerformanceIssue,
}

/// <summary>
///     Severity levels for alerts.
/// </summary>
public enum AlertSeverity
{
    /// <summary>
    ///     Informational alert.
    /// </summary>
    Info,

    /// <summary>
    ///     Warning alert.
    /// </summary>
    Warning,

    /// <summary>
    ///     Error alert.
    /// </summary>
    Error,

    /// <summary>
    ///     Critical alert.
    /// </summary>
    Critical,
}
