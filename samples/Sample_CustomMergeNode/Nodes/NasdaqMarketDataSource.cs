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
///     Source node for NASDAQ market data
/// </summary>
public class NasdaqMarketDataSource : SourceNode<MarketDataTick>
{
    private readonly TimeSpan _delay = TimeSpan.FromMilliseconds(25);
    private readonly ILogger<NasdaqMarketDataSource> _logger;
    private readonly Random _random = new();
    private readonly Dictionary<string, decimal> _symbolPrices = new();
    private readonly List<string> _symbols = new() { "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA" };

    /// <summary>
    ///     Initializes a new instance of NasdaqMarketDataSource class
    /// </summary>
    /// <param name="logger">The logger</param>
    public NasdaqMarketDataSource(ILogger<NasdaqMarketDataSource> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Initialize symbol prices
        foreach (var symbol in _symbols)
        {
            _symbolPrices[symbol] = 100 + (decimal)(_random.NextDouble() * 900);
        }
    }

    /// <summary>
    ///     Executes the source node to generate NASDAQ market data
    /// </summary>
    /// <param name="context">The pipeline context</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>A data pipe containing the market data</returns>
    public override IDataPipe<MarketDataTick> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting NASDAQ Market Data Source");

        try
        {
            var channel = Channel.CreateUnbounded<MarketDataTick>();
            var dataPipe = new ChannelDataPipe<MarketDataTick>(channel, "NASDAQ-Source");

            // Start generating data in the background
            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var tick in GenerateMarketDataAsync(cancellationToken))
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            _logger.LogInformation("NASDAQ market data generation cancelled");
                            break;
                        }

                        await dataPipe.WriteAsync(tick, cancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("NASDAQ market data generation cancelled");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error generating NASDAQ market data");
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
            _logger.LogError(ex, "Error starting NASDAQ Market Data Source");
            throw;
        }
    }

    /// <summary>
    ///     Generates market data asynchronously
    /// </summary>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>An async enumerable of market data ticks</returns>
    private async IAsyncEnumerable<MarketDataTick> GenerateMarketDataAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        const int batchCount = 12;

        for (var batch = 0; batch < batchCount && !cancellationToken.IsCancellationRequested; batch++)
        {
            foreach (var symbol in _symbols)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var basePrice = _symbolPrices[symbol];
                var priceChange = (decimal)(_random.NextDouble() * 5.0 - 2.5); // +/- $2.5
                var newPrice = basePrice + priceChange;

                _symbolPrices[symbol] = newPrice;

                var volume = (long)(_random.NextDouble() * 7500.0 + 2000.0);
                var timestamp = DateTime.UtcNow.AddMilliseconds(-_random.Next(0, 100));

                var qualityScore = new DataQualityScore(
                    85.0 + _random.NextDouble() * 10.0,
                    80.0 + _random.NextDouble() * 15.0,
                    90.0 + _random.NextDouble() * 8.0,
                    85.0 + _random.NextDouble() * 10.0
                );

                var tick = new MarketDataTick(
                    symbol,
                    newPrice,
                    volume,
                    timestamp,
                    "NASDAQ",
                    ExchangePriority.Medium,
                    qualityScore
                );

                _logger.LogDebug("Generated {Symbol} tick: {Price} at {Timestamp}", symbol, tick.Price, tick.Timestamp);

                yield return tick;

                await Task.Delay(_delay, cancellationToken);
            }

            await Task.Delay(TimeSpan.FromMilliseconds(150), cancellationToken);
        }

        _logger.LogInformation("NASDAQ market data generation completed after {BatchCount} batches", batchCount);
    }
}
