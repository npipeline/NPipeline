using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_TapNode.Nodes;

/// <summary>
///     Source node that generates transaction data for processing.
///     This node creates a realistic stream of financial transactions with various characteristics.
/// </summary>
public sealed class TransactionSource : SourceNode<Transaction>
{
    private readonly string[] _accountNumbers;
    private readonly string[] _categories;
    private readonly string[] _descriptions;
    private readonly ILogger<TransactionSource> _logger;
    private readonly string[] _merchants;
    private readonly Random _random = new();

    /// <summary>
    ///     Initializes a new instance of the <see cref="TransactionSource" /> class.
    /// </summary>
    /// <param name="logger">Logger for diagnostic information.</param>
    public TransactionSource(ILogger<TransactionSource> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Initialize test data
        _accountNumbers = new[]
        {
            "ACC001", "ACC002", "ACC003", "ACC004", "ACC005",
            "ACC006", "ACC007", "ACC008", "ACC009", "ACC010",
        };

        _merchants = new[]
        {
            "Amazon", "Walmart", "Target", "Starbucks", "McDonald's",
            "Home Depot", "Best Buy", "Netflix", "Spotify", "Gas Station",
        };

        _categories = new[]
        {
            "Food", "Transportation", "Entertainment", "Shopping", "Utilities",
            "Healthcare", "Education", "Travel", "Insurance", "Other",
        };

        _descriptions = new[]
        {
            "Online purchase", "In-store transaction", "Recurring payment", "ATM withdrawal",
            "Transfer to savings", "Bill payment", "Subscription fee", "Gas purchase",
            "Grocery shopping", "Restaurant meal",
        };
    }

    /// <inheritdoc />
    public override IDataPipe<Transaction> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("TransactionSource: Starting to generate transaction stream");

        try
        {
            var transactions = new List<Transaction>();
            var transactionCount = 0;
            var baseTime = DateTimeOffset.UtcNow.AddMinutes(-10); // Start 10 minutes ago

            // Generate 50 transactions with realistic timing
            for (var i = 0; i < 50; i++)
            {
                if (cancellationToken.IsCancellationRequested)
                    break;

                var transaction = GenerateTransaction(baseTime, i);
                transactions.Add(transaction);

                transactionCount++;

                // Log progress every 10 transactions
                if (transactionCount % 10 == 0)
                    _logger.LogInformation("TransactionSource: Generated {Count} transactions", transactionCount);
            }

            _logger.LogInformation("TransactionSource: Completed generating {Count} transactions", transactionCount);
            return new InMemoryDataPipe<Transaction>(transactions, "TransactionSource");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "TransactionSource: Error generating transaction stream");
            throw;
        }
    }

    /// <summary>
    ///     Generates a single transaction with realistic data.
    /// </summary>
    /// <param name="baseTime">Base time for transaction timestamps.</param>
    /// <param name="index">Index of the transaction for uniqueness.</param>
    /// <returns>A generated transaction.</returns>
    private Transaction GenerateTransaction(DateTimeOffset baseTime, int index)
    {
        var transactionType = GetRandomTransactionType();
        var amount = GenerateAmount(transactionType);
        var isFlagged = ShouldBeFlagged(amount, transactionType);
        var riskScore = CalculateRiskScore(amount, transactionType, isFlagged);

        return new Transaction
        {
            TransactionId = $"TXN{index:D6}",
            AccountNumber = _accountNumbers[_random.Next(_accountNumbers.Length)],
            Amount = amount,
            Type = transactionType,
            Timestamp = baseTime.AddSeconds(index * _random.Next(5, 30)),
            Description = _descriptions[_random.Next(_descriptions.Length)],
            Merchant = transactionType == TransactionType.Payment
                ? _merchants[_random.Next(_merchants.Length)]
                : null,
            Category = _categories[_random.Next(_categories.Length)],
            IsFlagged = isFlagged,
            RiskScore = riskScore,
        };
    }

    /// <summary>
    ///     Gets a random transaction type with weighted probabilities.
    /// </summary>
    /// <returns>A random transaction type.</returns>
    private TransactionType GetRandomTransactionType()
    {
        // Weighted distribution: Payment (40%), Deposit (25%), Withdrawal (20%), Transfer (15%)
        var roll = _random.Next(100);

        return roll switch
        {
            < 40 => TransactionType.Payment,
            < 65 => TransactionType.Deposit,
            < 85 => TransactionType.Withdrawal,
            _ => TransactionType.Transfer,
        };
    }

    /// <summary>
    ///     Generates a realistic amount based on transaction type.
    /// </summary>
    /// <param name="transactionType">The type of transaction.</param>
    /// <returns>A generated amount.</returns>
    private decimal GenerateAmount(TransactionType transactionType)
    {
        return transactionType switch
        {
            TransactionType.Payment => (decimal)(_random.NextDouble() * 200 + 5), // $5 - $205
            TransactionType.Deposit => (decimal)(_random.NextDouble() * 1000 + 50), // $50 - $1050
            TransactionType.Withdrawal => (decimal)(_random.NextDouble() * 500 + 20), // $20 - $520
            TransactionType.Transfer => (decimal)(_random.NextDouble() * 1500 + 100), // $100 - $1600
            _ => (decimal)(_random.NextDouble() * 100 + 1), // $1 - $101
        };
    }

    /// <summary>
    ///     Determines if a transaction should be flagged based on amount and type.
    /// </summary>
    /// <param name="amount">The transaction amount.</param>
    /// <param name="transactionType">The transaction type.</param>
    /// <returns>True if the transaction should be flagged.</returns>
    private bool ShouldBeFlagged(decimal amount, TransactionType transactionType)
    {
        // Flag high-value transactions or unusual patterns
        return transactionType switch
        {
            TransactionType.Payment => amount > 150, // High-value payments
            TransactionType.Deposit => amount > 800, // Large deposits
            TransactionType.Withdrawal => amount > 400, // Large withdrawals
            TransactionType.Transfer => amount > 1200, // Large transfers
            _ => false,
        };
    }

    /// <summary>
    ///     Calculates a risk score for the transaction.
    /// </summary>
    /// <param name="amount">The transaction amount.</param>
    /// <param name="transactionType">The transaction type.</param>
    /// <param name="isFlagged">Whether the transaction is flagged.</param>
    /// <returns>A risk score from 0-100.</returns>
    private int CalculateRiskScore(decimal amount, TransactionType transactionType, bool isFlagged)
    {
        var baseScore = _random.Next(10, 30); // Base risk score

        // Add risk based on amount
        var amountRisk = (int)Math.Min(amount / 10, 40);

        // Add risk based on transaction type
        var typeRisk = transactionType switch
        {
            TransactionType.Transfer => 15, // Transfers are higher risk
            TransactionType.Withdrawal => 10, // Withdrawals have moderate risk
            TransactionType.Payment => 5, // Payments have lower risk
            TransactionType.Deposit => 0, // Deposits have lowest risk
            _ => 5,
        };

        // Add risk if flagged
        var flaggedRisk = isFlagged
            ? 25
            : 0;

        var totalRisk = baseScore + amountRisk + typeRisk + flaggedRisk;

        return Math.Min(totalRisk, 100); // Cap at 100
    }
}
