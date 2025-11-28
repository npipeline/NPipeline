using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_UnbatchingNode.Nodes;

/// <summary>
///     Source node that generates individual market data events.
///     This node simulates a financial trading system receiving market data from multiple exchanges.
/// </summary>
public class MarketDataSource : SourceNode<MarketDataEvent>
{
    private readonly int _eventCount;
    private readonly string[] _eventTypes;
    private readonly string[] _exchanges;
    private readonly TimeSpan _interval;
    private readonly int _symbolCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MarketDataSource" /> class.
    /// </summary>
    /// <param name="eventCount">The number of market data events to generate.</param>
    /// <param name="interval">The interval between events.</param>
    /// <param name="symbolCount">The number of different trading symbols to simulate.</param>
    public MarketDataSource(int eventCount = 100, TimeSpan? interval = null, int symbolCount = 5)
    {
        _eventCount = eventCount;
        _interval = interval ?? TimeSpan.FromMilliseconds(50);
        _symbolCount = symbolCount;
        _exchanges = new[] { "NYSE", "NASDAQ", "LSE", "TSE", "HKEX" };
        _eventTypes = new[] { "Trade", "Quote", "Depth" };
    }

    /// <summary>
    ///     Generates a stream of market data events from multiple symbols and exchanges.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the market data events.</returns>
    public override IDataPipe<MarketDataEvent> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Generating {_eventCount} market data events from {_symbolCount} symbols with {_interval.TotalMilliseconds}ms intervals");

        var events = new List<MarketDataEvent>();
        var random = new Random(42); // Fixed seed for reproducible results
        var baseTime = DateTime.UtcNow;

        // Generate base prices for each symbol
        var basePrices = new Dictionary<string, decimal>();

        for (var i = 0; i < _symbolCount; i++)
        {
            var symbol = GetSymbol(i);
            basePrices[symbol] = 100m + i * 50m + (decimal)random.NextDouble() * 20m; // Base price between 100-200
        }

        for (var i = 0; i < _eventCount; i++)
        {
            var symbol = GetSymbol(i % _symbolCount);
            var exchange = _exchanges[random.Next(_exchanges.Length)];
            var eventType = _eventTypes[random.Next(_eventTypes.Length)];
            var timestamp = baseTime.AddMilliseconds(i * _interval.TotalMilliseconds);

            // Simulate realistic price movements
            var basePrice = basePrices[symbol];
            var priceChangePercent = (random.NextDouble() - 0.5) * 2.0; // ±1% change
            var priceChangeDecimal = (decimal)priceChangePercent / 100m;
            var currentPrice = basePrice * (1m + priceChangeDecimal);

            // Update base price for next event (momentum)
            basePrices[symbol] = currentPrice;

            // Generate realistic volume
            var volume = 100L + random.Next(0, 10000) * 100L;

            var marketEvent = new MarketDataEvent(
                $"Event-{i:D4}",
                timestamp,
                symbol,
                Math.Round(currentPrice, 2),
                volume,
                exchange,
                eventType);

            events.Add(marketEvent);

            // Simulate the interval between events
            if (i < _eventCount - 1) // Don't wait after the last event
                Task.Delay(_interval, cancellationToken).Wait(cancellationToken);
        }

        Console.WriteLine($"Successfully generated {events.Count} market data events");

        // Group by symbol to show the distribution
        var symbolGroups = events.GroupBy(e => e.Symbol).ToList();

        foreach (var group in symbolGroups)
        {
            Console.WriteLine($"  {group.Key}: {group.Count()} events");
        }

        return new ListDataPipe<MarketDataEvent>(events, "MarketDataSource");
    }

    private static string GetSymbol(int index)
    {
        var symbols = new[] { "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "JPM", "V", "JNJ" };
        return symbols[index % symbols.Length];
    }
}
