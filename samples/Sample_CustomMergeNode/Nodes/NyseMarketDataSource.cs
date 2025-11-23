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
///     Source node for NYSE market data
/// </summary>
public class NyseMarketDataSource : SourceNode<MarketDataTick>
{
    private readonly TimeSpan _delay = TimeSpan.FromMilliseconds(10);
    private readonly ILogger<NyseMarketDataSource> _logger;
    private readonly Random _random = new();
    private readonly Dictionary<string, decimal> _symbolPrices = new();
    private readonly List<string> _symbols = new() { "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA" };

    /// <summary>
    ///     Initializes a new instance of the NyseMarketDataSource class
    /// </summary>
    /// <param name="logger">The logger</param>
    public NyseMarketDataSource(ILogger<NyseMarketDataSource> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        // Initialize symbol prices
        foreach (var symbol in _symbols)
        {
            _symbolPrices[symbol] = 100 + (decimal)(_random.NextDouble() * 900);
        }
    }

    /// <summary>
    ///     Executes the source node to generate NYSE market data
    /// </summary>
    /// <param name="context">The pipeline context</param>
    /// <param name="cancellationToken">The cancellation token</param>
    /// <returns>A data pipe containing the market data</returns>
    public override IDataPipe<MarketDataTick> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting NYSE Market Data Source");

        try
        {
            var channel = Channel.CreateUnbounded<MarketDataTick>();
            var dataPipe = new ChannelDataPipe<MarketDataTick>(channel, "NYSE-Source");

            // Start generating data in the background
            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var tick in GenerateMarketDataAsync(cancellationToken))
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            _logger.LogInformation("NYSE market data generation cancelled");
                            break;
                        }

                        await dataPipe.WriteAsync(tick, cancellationToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("NYSE market data generation cancelled");
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error generating NYSE market data");
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
            _logger.LogError(ex, "Error starting NYSE Market Data Source");
            throw;
        }
    }

    /// <summary>
    ///     Generates realistic NYSE market data with high performance characteristics.
    ///     Optimized for high-frequency trading scenarios with minimal latency.
    /// </summary>
    private async IAsyncEnumerable<MarketDataTick> GenerateMarketDataAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Performance optimization: cache symbols array to avoid repeated allocation
        var symbolsCache = _symbols.ToArray();
        var symbolsCount = symbolsCache.Length;

        const int batchCount = 15;

        for (var batch = 0; batch < batchCount && !cancellationToken.IsCancellationRequested; batch++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            for (var i = 0; i < symbolsCount; i++)
            {
                var symbol = symbolsCache[i];

                var basePrice = _symbolPrices[symbol];
                var priceChange = (_random.NextDouble() - 0.5) * 2.0; // -1.0 to +1.0
                var newPrice = basePrice + (decimal)priceChange;

                _symbolPrices[symbol] = newPrice;

                var volume = (long)(_random.NextDouble() * 5000.0 + 1000.0);
                var timestamp = DateTime.UtcNow.AddMilliseconds(-_random.Next(0, 50));

                var qualityScore = new DataQualityScore(
                    95.0 + _random.NextDouble() * 5.0,
                    90.0 + _random.NextDouble() * 10.0,
                    98.0 + _random.NextDouble() * 2.0,
                    96.0 + _random.NextDouble() * 4.0
                );

                var tick = new MarketDataTick(
                    symbol,
                    newPrice,
                    volume,
                    timestamp,
                    "NYSE",
                    ExchangePriority.High,
                    qualityScore
                );

                yield return tick;

                await Task.Delay(_delay, cancellationToken);
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);
        }

        _logger?.LogInformation("NYSE market data generation completed after {BatchCount} batches", batchCount);
    }
}
