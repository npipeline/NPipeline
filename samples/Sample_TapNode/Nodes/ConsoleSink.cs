using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_TapNode.Nodes;

/// <summary>
///     Sink node that outputs processed transaction results to console.
///     This is the main sink for the primary processing pipeline.
/// </summary>
public sealed class ConsoleSink : SinkNode<ProcessedTransaction>
{
    // Console color codes (ANSI escape sequences)
    private const string ResetColor = "\x1b[0m";
    private const string Red = "\x1b[31m";
    private const string Green = "\x1b[32m";
    private const string Yellow = "\x1b[33m";
    private const string Blue = "\x1b[34m";
    private const string Magenta = "\x1b[35m";
    private const string Cyan = "\x1b[36m";
    private const string White = "\x1b[37m";
    private readonly ILogger<ConsoleSink> _logger;
    private int _approvedCount;
    private int _failedCount;
    private int _pendingReviewCount;
    private int _processedCount;
    private int _rejectedCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ConsoleSink" /> class.
    /// </summary>
    /// <param name="logger">Logger for diagnostic information.</param>
    public ConsoleSink(ILogger<ConsoleSink> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<ProcessedTransaction> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("ConsoleSink: Starting to output processed transactions");

        Console.WriteLine();
        Console.WriteLine("=== TRANSACTION PROCESSING RESULTS ===");
        Console.WriteLine();

        try
        {
            await foreach (var processedTransaction in input.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                OutputTransactionResult(processedTransaction);
                UpdateCounters(processedTransaction);
            }

            OutputSummary();

            _logger.LogInformation("ConsoleSink: Completed outputting {Count} processed transactions", _processedCount);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("ConsoleSink: Operation was cancelled");
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ConsoleSink: Error outputting transactions");
            throw;
        }
    }

    /// <summary>
    ///     Outputs a single transaction result to console.
    /// </summary>
    /// <param name="processedTransaction">The processed transaction to output.</param>
    private void OutputTransactionResult(ProcessedTransaction processedTransaction)
    {
        var transaction = processedTransaction.OriginalTransaction;
        var statusColor = GetStatusColor(processedTransaction.Status);
        var statusSymbol = GetStatusSymbol(processedTransaction.Status);

        Console.WriteLine($"{statusSymbol} [{statusColor}{processedTransaction.Status,-13}{ResetColor}] " +
                          $"ID: {transaction.TransactionId,-8} | " +
                          $"Account: {transaction.AccountNumber,-8} | " +
                          $"Amount: {transaction.Amount,10:C} | " +
                          $"Type: {transaction.Type,-10} | " +
                          $"Risk: {processedTransaction.FinalRiskScore,3} | " +
                          $"Time: {processedTransaction.ProcessingDurationMs,4}ms");

        // Output additional details for certain statuses
        if (processedTransaction.Status != ProcessingStatus.Approved)
        {
            var indent = new string(' ', 2);
            Console.WriteLine($"{indent}Details: {processedTransaction.ProcessingNotes ?? "No additional details"}");

            if (!transaction.IsFlagged && processedTransaction.FinalRiskScore > 70)
                Console.WriteLine($"{indent}Note: High risk score but not originally flagged");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Updates processing counters.
    /// </summary>
    /// <param name="processedTransaction">The processed transaction.</param>
    private void UpdateCounters(ProcessedTransaction processedTransaction)
    {
        _processedCount++;

        switch (processedTransaction.Status)
        {
            case ProcessingStatus.Approved:
                _approvedCount++;
                break;
            case ProcessingStatus.Rejected:
                _rejectedCount++;
                break;
            case ProcessingStatus.PendingReview:
                _pendingReviewCount++;
                break;
            case ProcessingStatus.Failed:
                _failedCount++;
                break;
        }
    }

    /// <summary>
    ///     Outputs a summary of all processed transactions.
    /// </summary>
    private void OutputSummary()
    {
        Console.WriteLine("=== PROCESSING SUMMARY ===");
        Console.WriteLine($"Total Processed: {_processedCount}");
        Console.WriteLine($"✅ Approved: {_approvedCount} ({(double)_approvedCount / _processedCount * 100:F1}%)");
        Console.WriteLine($"❌ Rejected: {_rejectedCount} ({(double)_rejectedCount / _processedCount * 100:F1}%)");
        Console.WriteLine($"⏳ Pending Review: {_pendingReviewCount} ({(double)_pendingReviewCount / _processedCount * 100:F1}%)");
        Console.WriteLine($"💥 Failed: {_failedCount} ({(double)_failedCount / _processedCount * 100:F1}%)");
        Console.WriteLine();
    }

    /// <summary>
    ///     Gets console color for a processing status.
    /// </summary>
    /// <param name="status">The processing status.</param>
    /// <returns>The console color code.</returns>
    private static string GetStatusColor(ProcessingStatus status)
    {
        return status switch
        {
            ProcessingStatus.Approved => Green,
            ProcessingStatus.Rejected => Red,
            ProcessingStatus.PendingReview => Yellow,
            ProcessingStatus.Failed => Magenta,
            _ => ResetColor,
        };
    }

    /// <summary>
    ///     Gets symbol for a processing status.
    /// </summary>
    /// <param name="status">The processing status.</param>
    /// <returns>The status symbol.</returns>
    private static string GetStatusSymbol(ProcessingStatus status)
    {
        return status switch
        {
            ProcessingStatus.Approved => "✅",
            ProcessingStatus.Rejected => "❌",
            ProcessingStatus.PendingReview => "⏳",
            ProcessingStatus.Failed => "💥",
            _ => "❓",
        };
    }
}
