using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_CustomMergeNode.Infrastructure;
using Sample_CustomMergeNode.Models;

namespace Sample_CustomMergeNode.Nodes;

/// <summary>
///     Source node for international market data with lower priority and higher latency
/// </summary>
public class InternationalMarketDataSource : SourceNode<MarketDataTick>
{
    private readonly ILogger<InternationalMarketDataSource> _logger;
    private readonly Random _random;
    private readonly Dictionary<string, decimal> _symbolPrices;
    private readonly List<string> _symbols;

    public InternationalMarketDataSource(ILogger<InternationalMarketDataSource> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _random = new Random();

        // Initialize symbols and prices
        _symbols = new List<string> { "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA" };
        _symbolPrices = new Dictionary<string, decimal>();

        // Initialize with base prices
        foreach (var symbol in _symbols)
        {
            _symbolPrices[symbol] = 100m + (decimal)(_random.NextDouble() * 200);
        }
    }

    public override IDataPipe<MarketDataTick> Execute(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting International Market Data Source");

        try
        {
            var channel = Channel.CreateUnbounded<MarketDataTick>();
            var dataPipe = new ChannelDataPipe<MarketDataTick>(channel, "International-Source");

            // Start generating data in the background
            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var tick in GenerateMarketDataAsync(cancellationToken))
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            _logger.LogInformation("International market data generation cancelled");
                            break;
                        }

                        await dataPipe.WriteAsync(tick, cancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("International market data generation cancelled");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error generating international market data");
                }
                finally
                {
                    _ = channel.Writer.TryComplete();
                }
            }, cancellationToken);

            return dataPipe;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error starting International Market Data Source");
            throw;
        }
    }

    private async IAsyncEnumerable<MarketDataTick> GenerateMarketDataAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var random = new Random();
        var basePrice = 150.0m;
        var symbols = new[] { "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA" };

        _logger?.LogInformation("International market data source started");

        for (var i = 0; i < 20 && !cancellationToken.IsCancellationRequested; i++)
        {
            var tick = await GenerateTickSafelyAsync(random, symbols, basePrice, cancellationToken);

            if (tick != null)
            {
                yield return tick;

                basePrice = tick.Price; // Update base price for next iteration
            }
        }

        _logger?.LogInformation("International market data source completed");
    }

    private async Task<MarketDataTick?> GenerateTickSafelyAsync(
        Random random,
        string[] symbols,
        decimal basePrice,
        CancellationToken cancellationToken)
    {
        try
        {
            var symbol = symbols[random.Next(symbols.Length)];

            // Simulate international market characteristics
            var priceChange = (random.NextDouble() - 0.5) * 2.0; // Higher volatility
            var newPrice = Math.Max(1.0m, basePrice + (decimal)priceChange);

            var volume = random.Next(1000, 25000);
            var timestamp = DateTime.UtcNow.AddMilliseconds(random.Next(-500, 500)); // More latency variation
            var priority = ExchangePriority.Low;

            // Occasionally simulate quality issues
            DataQualityScore? qualityScore = null;

            if (random.NextDouble() < 0.1) // 10% chance of quality issues
            {
                qualityScore = new DataQualityScore(
                    random.NextDouble() * 0.3 + 0.7,
                    random.NextDouble() * 0.4 + 0.6,
                    random.NextDouble() * 0.2 + 0.8,
                    random.NextDouble() * 0.3 + 0.7
                );
            }

            var tick = new MarketDataTick(symbol, newPrice, volume, timestamp, "International", priority, qualityScore);

            _logger?.LogDebug("Generated international market data: {Symbol} @ {Price:C} (Quality: {Quality})",
                symbol, newPrice, qualityScore?.OverallScore ?? 1.0);

            // Higher latency between updates
            await Task.Delay(random.Next(100, 250), cancellationToken);

            return tick;
        }
        catch (OperationCanceledException)
        {
            _logger?.LogInformation("International market data source cancelled");
            throw;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error generating international market data tick");
            return null; // Return null on error to continue with next iteration
        }
    }
}
