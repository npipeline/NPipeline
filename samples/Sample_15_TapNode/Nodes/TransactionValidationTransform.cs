using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_15_TapNode.Nodes;

/// <summary>
///     Transform node that validates transactions and determines processing status.
///     This is part of the main processing pipeline that validates business rules.
/// </summary>
public sealed class TransactionValidationTransform : TransformNode<Transaction, ValidatedTransaction>
{
    private readonly ILogger<TransactionValidationTransform> _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="TransactionValidationTransform" /> class.
    /// </summary>
    /// <param name="logger">Logger for diagnostic information.</param>
    public TransactionValidationTransform(ILogger<TransactionValidationTransform> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public override Task<ValidatedTransaction> ExecuteAsync(Transaction transaction, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogDebug("Validating transaction {TransactionId}", transaction.TransactionId);

        var validationResult = ValidateTransaction(transaction);

        var validatedTransaction = new ValidatedTransaction
        {
            OriginalTransaction = transaction,
            IsValid = validationResult.IsValid,
            ValidationErrors = validationResult.Errors,
            ValidationTimestamp = DateTimeOffset.UtcNow,
            ProcessingStatus = DetermineProcessingStatus(validationResult),
        };

        _logger.LogDebug(
            "Transaction {TransactionId} validation completed. Valid: {IsValid}, Status: {Status}",
            transaction.TransactionId,
            validationResult.IsValid,
            validatedTransaction.ProcessingStatus);

        return Task.FromResult(validatedTransaction);
    }

    /// <summary>
    ///     Validates a transaction against business rules.
    /// </summary>
    /// <param name="transaction">The transaction to validate.</param>
    /// <returns>Validation result with errors if any.</returns>
    private ValidationResult ValidateTransaction(Transaction transaction)
    {
        var errors = new List<string>();

        // Validate transaction ID
        if (string.IsNullOrWhiteSpace(transaction.TransactionId))
            errors.Add("Transaction ID is required");

        // Validate account number
        if (string.IsNullOrWhiteSpace(transaction.AccountNumber))
            errors.Add("Account number is required");
        else if (!transaction.AccountNumber.StartsWith("ACC", StringComparison.Ordinal))
            errors.Add("Account number must start with 'ACC'");

        // Validate amount
        if (transaction.Amount == 0)
            errors.Add("Transaction amount cannot be zero");
        else if (Math.Abs(transaction.Amount) > 10000)
            errors.Add("Transaction amount exceeds maximum limit of $10,000");

        // Validate timestamp
        if (transaction.Timestamp > DateTimeOffset.UtcNow.AddMinutes(5))
            errors.Add("Transaction timestamp cannot be more than 5 minutes in future");
        else if (transaction.Timestamp < DateTimeOffset.UtcNow.AddDays(-30))
            errors.Add("Transaction timestamp cannot be more than 30 days in past");

        // Validate description
        if (string.IsNullOrWhiteSpace(transaction.Description))
            errors.Add("Transaction description is required");

        // Type-specific validations
        ValidateTransactionType(transaction, errors);

        return new ValidationResult
        {
            IsValid = errors.Count == 0,
            Errors = errors,
        };
    }

    /// <summary>
    ///     Performs type-specific validation for transactions.
    /// </summary>
    /// <param name="transaction">The transaction to validate.</param>
    /// <param name="errors">List to add validation errors to.</param>
    private void ValidateTransactionType(Transaction transaction, List<string> errors)
    {
        switch (transaction.Type)
        {
            case TransactionType.Payment:
                if (string.IsNullOrWhiteSpace(transaction.Merchant))
                    errors.Add("Merchant is required for payment transactions");

                break;

            case TransactionType.Deposit:
                if (transaction.Amount <= 0)
                    errors.Add("Deposit amount must be positive");

                break;

            case TransactionType.Withdrawal:
                if (transaction.Amount >= 0)
                    errors.Add("Withdrawal amount must be negative");

                break;

            case TransactionType.Transfer:
                if (Math.Abs(transaction.Amount) < 10)
                    errors.Add("Transfer amount must be at least $10");

                break;
        }
    }

    /// <summary>
    ///     Determines the processing status based on validation results.
    /// </summary>
    /// <param name="validationResult">The validation result.</param>
    /// <returns>The processing status.</returns>
    private ProcessingStatus DetermineProcessingStatus(ValidationResult validationResult)
    {
        if (!validationResult.IsValid)
            return ProcessingStatus.Rejected;

        return ProcessingStatus.Approved;
    }

    /// <summary>
    ///     Represents the result of transaction validation.
    /// </summary>
    private sealed record ValidationResult
    {
        /// <summary>
        ///     Gets or sets whether the transaction is valid.
        /// </summary>
        public required bool IsValid { get; init; }

        /// <summary>
        ///     Gets or sets the list of validation errors.
        /// </summary>
        public required List<string> Errors { get; init; }
    }
}

/// <summary>
///     Represents a transaction that has been validated.
/// </summary>
public sealed record ValidatedTransaction
{
    /// <summary>
    ///     The original transaction.
    /// </summary>
    public required Transaction OriginalTransaction { get; init; }

    /// <summary>
    ///     Whether the transaction passed validation.
    /// </summary>
    public required bool IsValid { get; init; }

    /// <summary>
    ///     List of validation errors (if any).
    /// </summary>
    public required List<string> ValidationErrors { get; init; }

    /// <summary>
    ///     Timestamp when validation was performed.
    /// </summary>
    public required DateTimeOffset ValidationTimestamp { get; init; }

    /// <summary>
    ///     Initial processing status based on validation.
    /// </summary>
    public required ProcessingStatus ProcessingStatus { get; init; }
}
