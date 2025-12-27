using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_SelfJoinNode;

namespace Sample_SelfJoinNode.Nodes;

/// <summary>
///     Generic console sink node that outputs items to the console with formatting.
///     This node demonstrates how to create output sinks in NPipeline.
/// </summary>
/// <typeparam name="T">The type of items to output.</typeparam>
public class ConsoleSink<T> : SinkNode<T>
{
    private readonly ILogger<ConsoleSink<T>>? _logger;
    private readonly string _prefix;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ConsoleSink{T}" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    /// <param name="prefix">Optional prefix to add to each output line.</param>
    public ConsoleSink(ILogger<ConsoleSink<T>>? logger = null, string? prefix = null)
    {
        _logger = logger;
        _prefix = prefix ?? "Output";
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger?.LogInformation("ConsoleSink: Starting to output {Prefix} items", _prefix);

        var itemCount = 0;

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            itemCount++;

            if (item is null)
            {
                Console.WriteLine($"{_prefix}: [NULL]");
                continue;
            }

            // Format output based on item type
            var formattedOutput = FormatOutput(item);
            Console.WriteLine($"{_prefix}: {formattedOutput}");

            _logger?.LogDebug("ConsoleSink: Output item #{Count}: {Item}", itemCount, item);
        }

        Console.WriteLine();
        Console.WriteLine($"📋 {_prefix}: Processed {itemCount} items total");
        Console.WriteLine();

        _logger?.LogInformation("ConsoleSink: Completed outputting {Count} {Prefix} items", itemCount, _prefix);

        await Task.CompletedTask;
    }

    /// <summary>
    ///     Formats the output item for display based on its type.
    /// </summary>
    /// <param name="item">The item to format.</param>
    /// <returns>A formatted string representation of the item.</returns>
    private static string FormatOutput(T? item)
    {
        return item switch
        {
            YearOverYearComparison comparison => FormatYearOverYearComparison(comparison),
            CategorySummary summary => FormatCategorySummary(summary),
            SalesData sales => FormatSalesData(sales),
            null => $"[{typeof(T).Name}]",
            _ => item.ToString() ?? $"[{typeof(T).Name}]",
        };
    }

    /// <summary>
    ///     Formats a YearOverYearComparison for display.
    /// </summary>
    private static string FormatYearOverYearComparison(YearOverYearComparison comparison)
    {
        var growthStr = comparison.RevenueGrowthPercent.HasValue
            ? $"{comparison.RevenueGrowthPercent.Value:+0.0;-0.0}%"
            : "N/A";
        
        var unitsGrowthStr = comparison.UnitsGrowthPercent.HasValue
            ? $"{comparison.UnitsGrowthPercent.Value:+0.0;-0.0}%"
            : "N/A";

        return $"[{comparison.GrowthStatus}] {comparison.ProductName} ({comparison.CurrentYearSales.Category}) - " +
               $"Revenue: {comparison.CurrentYearSales.Revenue:C} (Δ {growthStr}), " +
               $"Units: {comparison.CurrentYearSales.UnitsSold} (Δ {unitsGrowthStr})";
    }

    /// <summary>
    ///     Formats a CategorySummary for display.
    /// </summary>
    private static string FormatCategorySummary(CategorySummary summary)
    {
        return $"Category: {summary.Category} - " +
               $"Products: {summary.TotalProducts}, " +
               $"Growing: {summary.GrowingProducts} ({summary.GrowthPercentage:F1}%), " +
               $"Declining: {summary.DecliningProducts}, " +
               $"New: {summary.NewProducts}, " +
               $"Avg Growth: {summary.AverageRevenueGrowth:+0.0;-0.0}%";
    }

    /// <summary>
    ///     Formats SalesData for display.
    /// </summary>
    private static string FormatSalesData(SalesData sales)
    {
        return $"Product {sales.ProductId}: {sales.ProductName} ({sales.Category}) - " +
               $"Year {sales.Year}, Revenue {sales.Revenue:C}, Units {sales.UnitsSold}";
    }
}
