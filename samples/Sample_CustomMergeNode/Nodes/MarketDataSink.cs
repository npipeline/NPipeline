using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_CustomMergeNode.Models;

namespace Sample_CustomMergeNode.Nodes;

/// <summary>
///     Sink node for market data
/// </summary>
public class MarketDataSink : SinkNode<MarketDataTick>
{
    private readonly ILogger<MarketDataSink> _logger;
    private readonly ConcurrentDictionary<string, List<MarketDataTick>> _processedTicks = new();
    private readonly ConcurrentDictionary<string, MarketDataStats> _symbolStats = new();
    private long _totalDropped;
    private long _totalProcessed;

    /// <summary>
    ///     Initializes a new instance of the MarketDataSink class
    /// </summary>
    /// <param name="logger">The logger</param>
    public MarketDataSink(ILogger<MarketDataSink> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    ///     Executes the sink node to process market data
    /// </summary>
    /// <param name="input">The input data pipe</param>
    /// <param name="context">The pipeline context</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>A task representing the execution</returns>
    public override async Task ExecuteAsync(IDataPipe<MarketDataTick> input, PipelineContext context, CancellationToken cancellationToken)
    {
        if (input == null)
        {
            _logger.LogWarning("Received null input in MarketDataSink");
            throw new ArgumentNullException(nameof(input));
        }

        _logger.LogInformation("Starting Market Data Sink");

        try
        {
            await foreach (var tick in input.WithCancellation(cancellationToken))
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    _logger.LogInformation("Market data sink processing cancelled");
                    break;
                }

                await ProcessTickAsync(tick, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing market data in sink");
            throw;
        }
    }

    /// <summary>
    ///     Processes a single market data tick
    /// </summary>
    /// <param name="tick">The market data tick to process</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>A task representing the processing</returns>
    private async Task ProcessTickAsync(MarketDataTick tick, CancellationToken cancellationToken)
    {
        if (tick == null)
        {
            _logger.LogWarning("Received null tick in MarketDataSink");
            return;
        }

        try
        {
            // Add to processed ticks
            _processedTicks.AddOrUpdate(tick.Symbol, new List<MarketDataTick> { tick }, (key, list) =>
            {
                list.Add(tick);
                return list;
            });

            // Update symbol statistics
            UpdateSymbolStats(tick);

            // Increment total processed
            Interlocked.Increment(ref _totalProcessed);

            // Log the processed tick
            _logger.LogInformation("Processed {Symbol} tick: {Price} ({Volume} shares) from {Exchange} [Quality: {QualityScore:P1}]",
                tick.Symbol, tick.Price, tick.Volume, tick.Exchange, tick.QualityScore?.OverallScore ?? 0.0);

            // Print detailed information every 10 ticks
            if (_totalProcessed % 10 == 0)
                PrintDetailedStatistics();

            await Task.CompletedTask;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing {Symbol} tick in MarketDataSink", tick.Symbol);
            Interlocked.Increment(ref _totalDropped);
        }
    }

    /// <summary>
    ///     Updates symbol statistics
    /// </summary>
    /// <param name="tick">The market data tick</param>
    private void UpdateSymbolStats(MarketDataTick tick)
    {
        _symbolStats.AddOrUpdate(tick.Symbol, new MarketDataStats
        {
            Symbol = tick.Symbol,
            Count = 1,
            TotalVolume = tick.Volume,
            MinPrice = tick.Price,
            MaxPrice = tick.Price,
            LastPrice = tick.Price,
            LastTimestamp = tick.Timestamp,
            Exchanges = new HashSet<string> { tick.Exchange },
        }, (key, stats) =>
        {
            stats.Count++;
            stats.TotalVolume += tick.Volume;
            stats.MinPrice = Math.Min(stats.MinPrice, tick.Price);
            stats.MaxPrice = Math.Max(stats.MaxPrice, tick.Price);
            stats.LastPrice = tick.Price;
            stats.LastTimestamp = tick.Timestamp;
            stats.Exchanges.Add(tick.Exchange);
            return stats;
        });
    }

    /// <summary>
    ///     Prints detailed statistics
    /// </summary>
    private void PrintDetailedStatistics()
    {
        _logger.LogInformation("=== Market Data Statistics ===");
        _logger.LogInformation("Total Processed: {TotalProcessed}", _totalProcessed);
        _logger.LogInformation("Total Dropped: {TotalDropped}", _totalDropped);
        _logger.LogInformation("Unique Symbols: {SymbolCount}", _symbolStats.Count);

        foreach (var kvp in _symbolStats)
        {
            var stats = kvp.Value;
            var avgPrice = (stats.MinPrice + stats.MaxPrice) / 2;

            _logger.LogInformation("  {Symbol}: {Count} ticks, Avg Price: {AvgPrice:F2}, Volume: {TotalVolume:N0}, Exchanges: {Exchanges}",
                stats.Symbol, stats.Count, avgPrice, stats.TotalVolume, string.Join(", ", stats.Exchanges));
        }
    }

    /// <summary>
    ///     Prints final statistics
    /// </summary>
    private void PrintFinalStatistics()
    {
        _logger.LogInformation("=== Final Market Data Statistics ===");
        _logger.LogInformation("Total Processed: {TotalProcessed}", _totalProcessed);
        _logger.LogInformation("Total Dropped: {TotalDropped}", _totalDropped);

        foreach (var kvp in _symbolStats)
        {
            var stats = kvp.Value;
            var avgPrice = (stats.MinPrice + stats.MaxPrice) / 2;
            var priceRange = stats.MaxPrice - stats.MinPrice;

            _logger.LogInformation(
                "  {Symbol}: {Count} ticks, Price Range: {MinPrice:F2}-{MaxPrice:F2} ({PriceRange:F2}), Volume: {TotalVolume:N0}, Exchanges: {Exchanges}",
                stats.Symbol, stats.Count, stats.MinPrice, stats.MaxPrice, priceRange, stats.TotalVolume, string.Join(", ", stats.Exchanges));
        }
    }

    /// <summary>
    ///     Gets the processed ticks
    /// </summary>
    /// <returns>A dictionary of processed ticks by symbol</returns>
    public ConcurrentDictionary<string, List<MarketDataTick>> GetProcessedTicks()
    {
        return _processedTicks;
    }

    /// <summary>
    ///     Gets the symbol statistics
    /// </summary>
    /// <returns>A dictionary of symbol statistics</returns>
    public ConcurrentDictionary<string, MarketDataStats> GetSymbolStats()
    {
        return _symbolStats;
    }
}

/// <summary>
///     Represents market data statistics for a symbol
/// </summary>
public class MarketDataStats
{
    /// <summary>
    ///     Gets or sets the symbol
    /// </summary>
    public string Symbol { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the count of ticks
    /// </summary>
    public int Count { get; set; }

    /// <summary>
    ///     Gets or sets the total volume
    /// </summary>
    public long TotalVolume { get; set; }

    /// <summary>
    ///     Gets or sets the minimum price
    /// </summary>
    public decimal MinPrice { get; set; }

    /// <summary>
    ///     Gets or sets the maximum price
    /// </summary>
    public decimal MaxPrice { get; set; }

    /// <summary>
    ///     Gets or sets the last price
    /// </summary>
    public decimal LastPrice { get; set; }

    /// <summary>
    ///     Gets or sets the last timestamp
    /// </summary>
    public DateTime LastTimestamp { get; set; }

    /// <summary>
    ///     Gets or sets the exchanges
    /// </summary>
    public HashSet<string> Exchanges { get; set; } = new();
}
