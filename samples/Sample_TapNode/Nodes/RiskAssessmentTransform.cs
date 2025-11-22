using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_TapNode.Nodes;

/// <summary>
///     Transform node that performs risk assessment on validated transactions.
///     This node analyzes transaction patterns and calculates final risk scores.
/// </summary>
public sealed class RiskAssessmentTransform : TransformNode<ValidatedTransaction, ProcessedTransaction>
{
    private readonly ILogger<RiskAssessmentTransform> _logger;
    private readonly Random _random = new();

    /// <summary>
    ///     Initializes a new instance of the <see cref="RiskAssessmentTransform" /> class.
    /// </summary>
    /// <param name="logger">Logger for diagnostic information.</param>
    public RiskAssessmentTransform(ILogger<RiskAssessmentTransform> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public override async Task<ProcessedTransaction> ExecuteAsync(ValidatedTransaction validatedTransaction, PipelineContext context,
        CancellationToken cancellationToken)
    {
        var transaction = validatedTransaction.OriginalTransaction;
        var startTime = DateTimeOffset.UtcNow;

        _logger.LogDebug("Performing risk assessment for transaction {TransactionId}", transaction.TransactionId);

        try
        {
            // Skip risk assessment for invalid transactions
            if (!validatedTransaction.IsValid)
                return CreateProcessedTransaction(validatedTransaction, startTime, 0, "Transaction validation failed");

            // Perform risk assessment
            var riskAssessment = await PerformRiskAssessment(validatedTransaction, cancellationToken).ConfigureAwait(false);

            // Determine final processing status
            var finalStatus = DetermineFinalStatus(validatedTransaction, riskAssessment);

            var processedTransaction = new ProcessedTransaction
            {
                OriginalTransaction = transaction,
                Status = finalStatus,
                ProcessedAt = DateTimeOffset.UtcNow,
                ProcessingDurationMs = (long)(DateTimeOffset.UtcNow - startTime).TotalMilliseconds,
                ProcessingNotes = riskAssessment.Notes,
                FinalRiskScore = riskAssessment.FinalRiskScore,
            };

            _logger.LogDebug(
                "Risk assessment completed for transaction {TransactionId}. Final Risk Score: {RiskScore}, Status: {Status}",
                transaction.TransactionId,
                riskAssessment.FinalRiskScore,
                finalStatus);

            return processedTransaction;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during risk assessment for transaction {TransactionId}", transaction.TransactionId);

            return CreateProcessedTransaction(validatedTransaction, startTime, transaction.RiskScore, $"Risk assessment failed: {ex.Message}");
        }
    }

    /// <summary>
    ///     Performs comprehensive risk assessment on the transaction.
    /// </summary>
    /// <param name="validatedTransaction">The validated transaction to assess.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Risk assessment result.</returns>
    private async Task<RiskAssessmentResult> PerformRiskAssessment(ValidatedTransaction validatedTransaction, CancellationToken cancellationToken)
    {
        var transaction = validatedTransaction.OriginalTransaction;
        var baseRiskScore = transaction.RiskScore;
        var riskFactors = new List<string>();

        // Simulate some processing time for risk assessment
        await Task.Delay(TimeSpan.FromMilliseconds(_random.Next(10, 50)), cancellationToken).ConfigureAwait(false);

        // Amount-based risk assessment
        var amountRisk = CalculateAmountRisk(transaction.Amount);
        riskFactors.AddRange(amountRisk.Factors);

        // Frequency-based risk assessment (simulated)
        var frequencyRisk = CalculateFrequencyRisk(transaction.AccountNumber);
        riskFactors.AddRange(frequencyRisk.Factors);

        // Pattern-based risk assessment
        var patternRisk = CalculatePatternRisk(transaction);
        riskFactors.AddRange(patternRisk.Factors);

        // Time-based risk assessment
        var timeRisk = CalculateTimeRisk(transaction.Timestamp);
        riskFactors.AddRange(timeRisk.Factors);

        // Calculate final risk score
        var finalRiskScore = CalculateFinalRiskScore(baseRiskScore, amountRisk.Score, frequencyRisk.Score, patternRisk.Score, timeRisk.Score);

        // Generate assessment notes
        var notes = GenerateAssessmentNotes(finalRiskScore, riskFactors);

        return new RiskAssessmentResult
        {
            FinalRiskScore = finalRiskScore,
            RiskFactors = riskFactors,
            Notes = notes,
        };
    }

    /// <summary>
    ///     Calculates risk based on transaction amount.
    /// </summary>
    /// <param name="amount">The transaction amount.</param>
    /// <returns>Amount-based risk assessment.</returns>
    private RiskFactor CalculateAmountRisk(decimal amount)
    {
        var factors = new List<string>();
        var score = 0;

        var absoluteAmount = Math.Abs(amount);

        if (absoluteAmount > 5000)
        {
            factors.Add("Very high transaction amount");
            score += 30;
        }
        else if (absoluteAmount > 2000)
        {
            factors.Add("High transaction amount");
            score += 20;
        }
        else if (absoluteAmount > 1000)
        {
            factors.Add("Moderate transaction amount");
            score += 10;
        }

        // Round numbers are less risky
        if (absoluteAmount % 100 == 0)
        {
            score -= 5;
            factors.Add("Round amount (lower risk)");
        }

        return new RiskFactor { Score = score, Factors = factors };
    }

    /// <summary>
    ///     Calculates risk based on transaction frequency (simulated).
    /// </summary>
    /// <param name="accountNumber">The account number.</param>
    /// <returns>Frequency-based risk assessment.</returns>
    private RiskFactor CalculateFrequencyRisk(string accountNumber)
    {
        var factors = new List<string>();
        var score = 0;

        // Simulate frequency check based on account number hash
        var hash = accountNumber.GetHashCode();
        var simulatedTransactionCount = Math.Abs(hash % 20);

        if (simulatedTransactionCount > 15)
        {
            factors.Add("High transaction frequency");
            score += 15;
        }
        else if (simulatedTransactionCount > 10)
        {
            factors.Add("Moderate transaction frequency");
            score += 8;
        }

        return new RiskFactor { Score = score, Factors = factors };
    }

    /// <summary>
    ///     Calculates risk based on transaction patterns.
    /// </summary>
    /// <param name="transaction">The transaction to analyze.</param>
    /// <returns>Pattern-based risk assessment.</returns>
    private RiskFactor CalculatePatternRisk(Transaction transaction)
    {
        var factors = new List<string>();
        var score = 0;

        // Check for suspicious patterns
        if (transaction.Type == TransactionType.Transfer && Math.Abs(transaction.Amount) > 1000)
        {
            factors.Add("Large transfer transaction");
            score += 12;
        }

        if (transaction.Type == TransactionType.Withdrawal && Math.Abs(transaction.Amount) > 300)
        {
            factors.Add("Large cash withdrawal");
            score += 10;
        }

        if (transaction.Category == "Other" && Math.Abs(transaction.Amount) > 500)
        {
            factors.Add("High-value uncategorized transaction");
            score += 8;
        }

        // Positive patterns (reduce risk)
        if (transaction.Category == "Utilities" || transaction.Category == "Insurance")
        {
            factors.Add("Regular bill payment (lower risk)");
            score -= 5;
        }

        return new RiskFactor { Score = score, Factors = factors };
    }

    /// <summary>
    ///     Calculates risk based on transaction timing.
    /// </summary>
    /// <param name="timestamp">The transaction timestamp.</param>
    /// <returns>Time-based risk assessment.</returns>
    private RiskFactor CalculateTimeRisk(DateTimeOffset timestamp)
    {
        var factors = new List<string>();
        var score = 0;

        var hour = timestamp.Hour;

        // Late night transactions are slightly riskier
        if (hour >= 23 || hour <= 5)
        {
            factors.Add("Late night transaction");
            score += 5;
        }

        // Weekend transactions are slightly riskier
        if (timestamp.DayOfWeek == DayOfWeek.Saturday || timestamp.DayOfWeek == DayOfWeek.Sunday)
        {
            factors.Add("Weekend transaction");
            score += 3;
        }

        return new RiskFactor { Score = score, Factors = factors };
    }

    /// <summary>
    ///     Calculates the final risk score from all risk factors.
    /// </summary>
    /// <param name="baseScore">The base risk score.</param>
    /// <param name="riskScores">Additional risk scores.</param>
    /// <returns>The final risk score.</returns>
    private int CalculateFinalRiskScore(int baseScore, params int[] riskScores)
    {
        var totalScore = baseScore;

        foreach (var score in riskScores)
        {
            totalScore += score;
        }

        return Math.Max(0, Math.Min(100, totalScore)); // Clamp between 0 and 100
    }

    /// <summary>
    ///     Generates assessment notes based on risk factors.
    /// </summary>
    /// <param name="finalRiskScore">The final risk score.</param>
    /// <param name="riskFactors">List of risk factors.</param>
    /// <returns>Assessment notes.</returns>
    private string GenerateAssessmentNotes(int finalRiskScore, List<string> riskFactors)
    {
        var notes = $"Risk assessment completed. Final score: {finalRiskScore}";

        if (riskFactors.Count > 0)
        {
            notes += ". Factors: " + string.Join(", ", riskFactors.Take(3)); // Limit to top 3 factors

            if (riskFactors.Count > 3)
                notes += $" (+{riskFactors.Count - 3} more)";
        }

        return notes;
    }

    /// <summary>
    ///     Determines the final processing status based on validation and risk assessment.
    /// </summary>
    /// <param name="validatedTransaction">The validated transaction.</param>
    /// <param name="riskAssessment">The risk assessment result.</param>
    /// <returns>The final processing status.</returns>
    private ProcessingStatus DetermineFinalStatus(ValidatedTransaction validatedTransaction, RiskAssessmentResult riskAssessment)
    {
        // If validation failed, keep the rejected status
        if (!validatedTransaction.IsValid)
            return ProcessingStatus.Rejected;

        // High-risk transactions require manual review
        if (riskAssessment.FinalRiskScore > 80)
            return ProcessingStatus.PendingReview;

        // Medium-high risk transactions might be flagged but approved
        if (riskAssessment.FinalRiskScore > 60)
            return ProcessingStatus.Approved; // Could also be PendingReview based on business rules

        return ProcessingStatus.Approved;
    }

    /// <summary>
    ///     Creates a processed transaction with error information.
    /// </summary>
    /// <param name="validatedTransaction">The validated transaction.</param>
    /// <param name="startTime">Processing start time.</param>
    /// <param name="riskScore">Risk score to use.</param>
    /// <param name="notes">Processing notes.</param>
    /// <returns>A processed transaction with error information.</returns>
    private ProcessedTransaction CreateProcessedTransaction(ValidatedTransaction validatedTransaction, DateTimeOffset startTime, int riskScore, string notes)
    {
        return new ProcessedTransaction
        {
            OriginalTransaction = validatedTransaction.OriginalTransaction,
            Status = ProcessingStatus.Failed,
            ProcessedAt = DateTimeOffset.UtcNow,
            ProcessingDurationMs = (long)(DateTimeOffset.UtcNow - startTime).TotalMilliseconds,
            ProcessingNotes = notes,
            FinalRiskScore = riskScore,
        };
    }

    /// <summary>
    ///     Represents a risk assessment result.
    /// </summary>
    private sealed record RiskAssessmentResult
    {
        /// <summary>
        ///     The final calculated risk score.
        /// </summary>
        public required int FinalRiskScore { get; init; }

        /// <summary>
        ///     List of identified risk factors.
        /// </summary>
        public required List<string> RiskFactors { get; init; }

        /// <summary>
        ///     Assessment notes.
        /// </summary>
        public required string Notes { get; init; }
    }

    /// <summary>
    ///     Represents a risk factor with its score and description.
    /// </summary>
    private sealed record RiskFactor
    {
        /// <summary>
        ///     The risk score contribution.
        /// </summary>
        public required int Score { get; init; }

        /// <summary>
        ///     List of risk factor descriptions.
        /// </summary>
        public required List<string> Factors { get; init; }
    }
}
