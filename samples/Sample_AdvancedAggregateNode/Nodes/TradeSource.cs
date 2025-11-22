using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_AdvancedAggregateNode.Models;

namespace Sample_AdvancedAggregateNode.Nodes;

/// <summary>
///     Source node that generates simulated real-time financial trades.
///     This node simulates various types of financial instruments and trading activities.
/// </summary>
public class TradeSource : SourceNode<FinancialTrade>
{
    private readonly string[] _assetClasses =
    {
        FinancialConstants.Equity,
        FinancialConstants.FixedIncome,
        FinancialConstants.Derivatives,
        FinancialConstants.Commodities,
        FinancialConstants.Forex,
        FinancialConstants.Crypto,
    };

    private readonly Dictionary<string, decimal> _basePrices = new()
    {
        { "AAPL", 150.0m }, { "GOOGL", 2500.0m }, { "MSFT", 300.0m }, { "AMZN", 3200.0m },
        { "TSLA", 800.0m }, { "META", 250.0m }, { "NVDA", 200.0m }, { "JPM", 140.0m },
        { "JNJ", 160.0m }, { "V", 220.0m }, { "WMT", 140.0m }, { "PG", 140.0m },
        { "UNH", 450.0m }, { "HD", 320.0m }, { "MA", 350.0m }, { "BAC", 35.0m },
        { "XOM", 60.0m }, { "PFE", 45.0m }, { "CSCO", 50.0m }, { "ADBE", 500.0m },
    };

    private readonly string[] _portfolioIds =
    {
        "portfolio_equity_growth", "portfolio_fixed_income", "portfolio_mixed_balanced",
        "portfolio_aggressive_growth", "portfolio_conservative", "portfolio_tech_focus",
        "portfolio_commodities", "portfolio_forex_trading", "portfolio_crypto_assets",
        "portfolio_derivatives_hedging",
    };

    private readonly Random _random = new();

    private readonly string[] _symbols =
    {
        "AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NVDA", "JPM", "JNJ", "V",
        "WMT", "PG", "UNH", "HD", "MA", "BAC", "XOM", "PFE", "CSCO", "ADBE",
        "CRM", "NFLX", "CMCSA", "ACN", "KO", "PEP", "TMO", "COST", "AVGO", "LIN",
    };

    private readonly string[] _traderIds =
    {
        "trader_001", "trader_002", "trader_003", "trader_004", "trader_005",
        "trader_006", "trader_007", "trader_008", "trader_009", "trader_010",
    };

    /// <summary>
    ///     Generates a stream of simulated financial trades with realistic market data.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token to stop trade generation.</param>
    /// <returns>A data pipe containing financial trades.</returns>
    public override IDataPipe<FinancialTrade> ExecuteAsync(
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine("TradeSource: Starting to generate financial trades...");
        Console.WriteLine("TradeSource: Simulating real-time market trading activity...");

        var tradeCount = 0;
        var startTime = DateTime.UtcNow;

        async IAsyncEnumerable<FinancialTrade> GenerateTrades([EnumeratorCancellation] CancellationToken ct = default)
        {
            while (!ct.IsCancellationRequested)
            {
                var trade = GenerateRandomTrade(tradeCount++);
                yield return trade;

                // Simulate variable trade frequency (5-20 trades per second on average)
                var delayMs = _random.Next(50, 200);
                await Task.Delay(delayMs, ct);

                // Log progress every 100 trades
                if (tradeCount % 100 == 0)
                {
                    var elapsed = DateTime.UtcNow - startTime;
                    var rate = tradeCount / elapsed.TotalSeconds;
                    Console.WriteLine($"TradeSource: Generated {tradeCount} trades ({rate:F1} trades/sec)");
                }
            }

            Console.WriteLine($"TradeSource: Stopped after generating {tradeCount} trades");
        }

        return new StreamingDataPipe<FinancialTrade>(GenerateTrades(cancellationToken), "TradeSource");
    }

    /// <summary>
    ///     Generates a single random financial trade with realistic data.
    /// </summary>
    /// <param name="sequenceNumber">The sequence number for the trade.</param>
    /// <returns>A randomly generated financial trade.</returns>
    private FinancialTrade GenerateRandomTrade(int sequenceNumber)
    {
        var symbol = _symbols[_random.Next(_symbols.Length)];
        var assetClass = GetAssetClassForSymbol(symbol);
        var traderId = _traderIds[_random.Next(_traderIds.Length)];
        var portfolioId = _portfolioIds[_random.Next(_portfolioIds.Length)];

        var tradeType = _random.Next(0, 2) == 0
            ? FinancialConstants.Buy
            : FinancialConstants.Sell;

        var timestamp = DateTime.UtcNow.AddSeconds(-_random.Next(0, 600)); // Trades from last 10 minutes

        // Generate realistic price based on base price with market volatility
        var basePrice = _basePrices.GetValueOrDefault(symbol, 100.0m);
        var priceVariation = (decimal)(_random.NextDouble() - 0.5) * 0.1m; // ±5% variation
        var price = basePrice * (1 + priceVariation);

        // Generate quantity based on asset class
        var quantity = GenerateQuantityForAssetClass(assetClass);
        var notionalValue = price * quantity;

        // Generate trade-specific properties
        var properties = GeneratePropertiesForTrade(assetClass, tradeType);

        return new FinancialTrade(
            $"trade_{sequenceNumber:D6}",
            symbol,
            assetClass,
            price,
            quantity,
            notionalValue,
            timestamp,
            tradeType,
            traderId,
            portfolioId,
            properties
        );
    }

    /// <summary>
    ///     Gets the appropriate asset class for a given symbol.
    /// </summary>
    /// <param name="symbol">The trading symbol.</param>
    /// <returns>The corresponding asset class.</returns>
    private string GetAssetClassForSymbol(string symbol)
    {
        // Most symbols are equities, with some exceptions for demonstration
        return symbol switch
        {
            "BTC" or "ETH" => FinancialConstants.Crypto,
            "EUR" or "GBP" or "JPY" => FinancialConstants.Forex,
            "GOLD" or "OIL" => FinancialConstants.Commodities,
            _ => FinancialConstants.Equity,
        };
    }

    /// <summary>
    ///     Generates a realistic quantity based on the asset class.
    /// </summary>
    /// <param name="assetClass">The asset class.</param>
    /// <returns>A realistic quantity for the asset class.</returns>
    private decimal GenerateQuantityForAssetClass(string assetClass)
    {
        return assetClass switch
        {
            FinancialConstants.Equity => _random.Next(10, 1000), // 10-1000 shares
            FinancialConstants.FixedIncome => (decimal)(_random.NextDouble() * 1000000 + 100000), // 100K-1.1M face value
            FinancialConstants.Derivatives => _random.Next(1, 100), // 1-100 contracts
            FinancialConstants.Commodities => _random.Next(50, 500), // 50-500 units
            FinancialConstants.Forex => (decimal)(_random.NextDouble() * 1000000 + 100000), // 100K-1.1M units
            FinancialConstants.Crypto => (decimal)(_random.NextDouble() * 10) + 0.1m, // 0.1-10.1 units
            _ => _random.Next(1, 100),
        };
    }

    /// <summary>
    ///     Generates trade-specific properties for realistic simulation.
    /// </summary>
    /// <param name="assetClass">The asset class.</param>
    /// <param name="tradeType">The trade type (BUY/SELL).</param>
    /// <returns>A dictionary of trade properties.</returns>
    private Dictionary<string, object> GeneratePropertiesForTrade(string assetClass, string tradeType)
    {
        var properties = new Dictionary<string, object>
        {
            ["exchange"] = _random.Next(0, 4) switch { 0 => "NYSE", 1 => "NASDAQ", 2 => "LSE", _ => "TSE" },
            ["market_session"] = _random.Next(0, 3) switch { 0 => "pre_market", 1 => "regular", 2 => "after_hours", _ => "regular" },
            ["order_type"] = _random.Next(0, 3) switch { 0 => "market", 1 => "limit", 2 => "stop", _ => "market" },
            ["execution_venue"] = _random.Next(0, 2) == 0
                ? "electronic"
                : "floor",
            ["settlement_date"] = DateTime.UtcNow.AddDays(_random.Next(1, 4)).ToString("yyyy-MM-dd"),
        };

        switch (assetClass)
        {
            case FinancialConstants.Equity:
                properties["sector"] = _random.Next(0, 6) switch
                {
                    0 => "Technology", 1 => "Healthcare", 2 => "Finance",
                    3 => "Consumer", 4 => "Energy", _ => "Industrial",
                };

                properties["market_cap"] = _random.Next(0, 3) switch { 0 => "Large", 1 => "Mid", 2 => "Small", _ => "Large" };
                break;

            case FinancialConstants.FixedIncome:
                properties["credit_rating"] = _random.Next(0, 4) switch { 0 => "AAA", 1 => "AA", 2 => "A", _ => "BBB" };
                properties["maturity"] = $"{_random.Next(1, 30)}Y";
                properties["coupon_rate"] = $"{_random.NextDouble() * 5 + 1:F2}%";
                break;

            case FinancialConstants.Derivatives:
                properties["contract_type"] = _random.Next(0, 3) switch { 0 => "Call", 1 => "Put", 2 => "Future", _ => "Call" };
                properties["strike_price"] = _basePrices[_symbols[_random.Next(_symbols.Length)]] * (1 + (decimal)(_random.NextDouble() - 0.5) * 0.2m);
                properties["expiry_date"] = DateTime.UtcNow.AddDays(_random.Next(30, 365)).ToString("yyyy-MM-dd");
                break;

            case FinancialConstants.Commodities:
                properties["commodity_type"] = _random.Next(0, 4) switch { 0 => "Energy", 1 => "Metals", 2 => "Agriculture", _ => "Livestock" };
                properties["delivery_location"] = _random.Next(0, 3) switch { 0 => "Cushing", 1 => "NYMEX", 2 => "CBOT", _ => "Cushing" };
                break;

            case FinancialConstants.Forex:
                properties["currency_pair"] = $"{_symbols[_random.Next(10)]}/{_symbols[_random.Next(10, 20)]}";
                properties["spot_rate"] = 1 + _random.NextDouble() * 0.5;
                break;

            case FinancialConstants.Crypto:
                properties["blockchain"] = _random.Next(0, 3) switch { 0 => "Bitcoin", 1 => "Ethereum", 2 => "Solana", _ => "Bitcoin" };
                properties["wallet_address"] = $"0x{_random.Next(100000000, 999999999):X16}";
                break;
        }

        return properties;
    }
}
