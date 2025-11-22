using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_AdvancedAggregateNode.Models;

namespace Sample_AdvancedAggregateNode.Nodes;

/// <summary>
///     Transform node that validates and enriches financial trades for risk analysis.
///     This node performs comprehensive validation and adds enrichment data for downstream processing.
/// </summary>
public class TradeValidationTransform : TransformNode<FinancialTrade, ValidatedTrade>
{
    private readonly Dictionary<string, decimal> _marketVolatilityScores = new();
    private readonly Random _random = new();
    private int _invalidTrades;
    private int _totalTradesProcessed;
    private int _validTrades;

    /// <summary>
    ///     Initializes a new instance of the TradeValidationTransform.
    /// </summary>
    public TradeValidationTransform()
    {
        Console.WriteLine("TradeValidationTransform: Initialized with comprehensive validation rules");
        Console.WriteLine("TradeValidationTransform: Will enrich trades with volatility and risk scoring");
        InitializeMarketVolatilityScores();
    }

    /// <summary>
    ///     Processes and validates a single financial trade, adding enrichment data.
    /// </summary>
    /// <param name="item">The input financial trade to validate and enrich.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token for async operation.</param>
    /// <returns>A validated and enriched trade.</returns>
    public override Task<ValidatedTrade> ExecuteAsync(
        FinancialTrade item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _totalTradesProcessed++;
        var validationResult = ValidateTrade(item);

        if (validationResult.IsValid)
            _validTrades++;
        else
            _invalidTrades++;

        // Log validation statistics every 50 trades
        if (_totalTradesProcessed % 50 == 0)
        {
            var validationRate = (double)_validTrades / _totalTradesProcessed * 100;

            Console.WriteLine($"TradeValidationTransform: Processed {_totalTradesProcessed} trades, " +
                              $"{validationRate:F1}% valid, {_invalidTrades} filtered");
        }

        return Task.FromResult(validationResult);
    }

    /// <summary>
    ///     Validates a financial trade and enriches it with additional data.
    /// </summary>
    /// <param name="trade">The trade to validate.</param>
    /// <returns>A validated trade with enrichment data.</returns>
    private ValidatedTrade ValidateTrade(FinancialTrade trade)
    {
        // Perform comprehensive validation
        var validationReason = ValidateTradeRules(trade);
        var isValid = validationReason == FinancialConstants.ValidTrade;

        // Calculate enrichment data even for invalid trades (for analysis purposes)
        var enrichedPrice = CalculateEnrichedPrice(trade);
        var volatilityScore = CalculateVolatilityScore(trade);
        var riskWeight = CalculateRiskWeight(trade, volatilityScore);

        return new ValidatedTrade(
            trade,
            isValid,
            validationReason,
            enrichedPrice,
            volatilityScore,
            riskWeight
        );
    }

    /// <summary>
    ///     Validates trade against business rules and market regulations.
    /// </summary>
    /// <param name="trade">The trade to validate.</param>
    /// <returns>The validation reason.</returns>
    private string ValidateTradeRules(FinancialTrade trade)
    {
        // Price validation
        if (trade.Price <= 0)
            return FinancialConstants.InvalidPrice;

        if (trade.Price > 1000000m) // Extremely high price threshold
            return FinancialConstants.InvalidPrice;

        // Quantity validation
        if (trade.Quantity <= 0)
            return FinancialConstants.InvalidQuantity;

        if (trade.Quantity > 10000000) // Extremely high quantity threshold
            return FinancialConstants.InvalidQuantity;

        // Notional value validation
        if (trade.NotionalValue <= 0)
            return FinancialConstants.InvalidPrice;

        if (trade.NotionalValue > 1000000000m) // $1B threshold
            return FinancialConstants.InvalidPrice;

        // Symbol validation
        if (string.IsNullOrWhiteSpace(trade.Symbol) || trade.Symbol.Length < 1 || trade.Symbol.Length > 10)
            return FinancialConstants.InvalidSymbol;

        // Required field validation
        if (string.IsNullOrWhiteSpace(trade.TraderId) ||
            string.IsNullOrWhiteSpace(trade.PortfolioId) ||
            string.IsNullOrWhiteSpace(trade.AssetClass))
            return FinancialConstants.MissingData;

        // Trading hours validation (simplified)
        var tradeTime = trade.Timestamp.TimeOfDay;
        var marketOpen = new TimeSpan(9, 30, 0);
        var marketClose = new TimeSpan(16, 0, 0);

        if (trade.AssetClass == FinancialConstants.Equity &&
            (tradeTime < marketOpen || tradeTime > marketClose))
        {
            // Allow some after-hours trading but flag it
            if (tradeTime < new TimeSpan(4, 0, 0) || tradeTime > new TimeSpan(20, 0, 0))
                return FinancialConstants.OutOfHours;
        }

        return FinancialConstants.ValidTrade;
    }

    /// <summary>
    ///     Calculates an enriched price based on market conditions and trade attributes.
    /// </summary>
    /// <param name="trade">The trade to enrich.</param>
    /// <returns>The enriched price.</returns>
    private decimal CalculateEnrichedPrice(FinancialTrade trade)
    {
        // Base enrichment: adjust for market volatility and liquidity
        var volatilityAdjustment = GetVolatilityAdjustment(trade.Symbol);
        var liquidityAdjustment = GetLiquidityAdjustment(trade.Quantity, trade.AssetClass);
        var timeAdjustment = GetTimeBasedAdjustment(trade.Timestamp.DateTime);

        var adjustmentFactor = 1.0m + volatilityAdjustment + liquidityAdjustment + timeAdjustment;
        return trade.Price * adjustmentFactor;
    }

    /// <summary>
    ///     Calculates a volatility score for the trade based on symbol and market conditions.
    /// </summary>
    /// <param name="trade">The trade to score.</param>
    /// <returns>A volatility score between 0 and 1.</returns>
    private decimal CalculateVolatilityScore(FinancialTrade trade)
    {
        var baseVolatility = _marketVolatilityScores.GetValueOrDefault(trade.Symbol, 0.2m);

        // Adjust for asset class
        var assetClassMultiplier = trade.AssetClass switch
        {
            FinancialConstants.Equity => 1.0m,
            FinancialConstants.Derivatives => 1.5m,
            FinancialConstants.Crypto => 2.0m,
            FinancialConstants.Forex => 0.8m,
            FinancialConstants.Commodities => 1.2m,
            FinancialConstants.FixedIncome => 0.3m,
            _ => 1.0m,
        };

        // Adjust for trade size (larger trades may indicate market impact)
        var sizeMultiplier = trade.Quantity > 10000
            ? 1.2m
            : 1.0m;

        var volatilityScore = baseVolatility * assetClassMultiplier * sizeMultiplier;
        return Math.Min(volatilityScore, 1.0m); // Cap at 1.0
    }

    /// <summary>
    ///     Calculates a risk weight for the trade based on various factors.
    /// </summary>
    /// <param name="trade">The trade to weight.</param>
    /// <param name="volatilityScore">The volatility score of the trade.</param>
    /// <returns>A risk weight for risk calculations.</returns>
    private decimal CalculateRiskWeight(FinancialTrade trade, decimal volatilityScore)
    {
        // Base risk weight from notional value (normalized)
        var notionalWeight = Math.Min(trade.NotionalValue / 1000000m, 1.0m); // Normalize to $1M

        // Combine with volatility score
        var combinedWeight = (notionalWeight + volatilityScore) / 2.0m;

        // Apply asset class risk factor
        var assetClassRisk = trade.AssetClass switch
        {
            FinancialConstants.Equity => 1.0m,
            FinancialConstants.Derivatives => 1.8m,
            FinancialConstants.Crypto => 2.5m,
            FinancialConstants.Forex => 0.9m,
            FinancialConstants.Commodities => 1.3m,
            FinancialConstants.FixedIncome => 0.4m,
            _ => 1.0m,
        };

        return combinedWeight * assetClassRisk;
    }

    /// <summary>
    ///     Initializes market volatility scores for different symbols.
    /// </summary>
    private void InitializeMarketVolatilityScores()
    {
        // Initialize with realistic volatility scores (0-1 scale)
        _marketVolatilityScores["AAPL"] = 0.25m;
        _marketVolatilityScores["GOOGL"] = 0.30m;
        _marketVolatilityScores["MSFT"] = 0.22m;
        _marketVolatilityScores["AMZN"] = 0.35m;
        _marketVolatilityScores["TSLA"] = 0.50m;
        _marketVolatilityScores["META"] = 0.40m;
        _marketVolatilityScores["NVDA"] = 0.45m;
        _marketVolatilityScores["JPM"] = 0.28m;
        _marketVolatilityScores["JNJ"] = 0.18m;
        _marketVolatilityScores["V"] = 0.20m;

        Console.WriteLine($"TradeValidationTransform: Initialized {_marketVolatilityScores.Count} market volatility scores");
    }

    /// <summary>
    ///     Gets volatility adjustment factor for a symbol.
    /// </summary>
    /// <param name="symbol">The trading symbol.</param>
    /// <returns>Volatility adjustment factor.</returns>
    private decimal GetVolatilityAdjustment(string symbol)
    {
        var volatility = _marketVolatilityScores.GetValueOrDefault(symbol, 0.25m);
        return (decimal)((_random.NextDouble() - 0.5) * (double)volatility * 0.02); // ±1% of volatility
    }

    /// <summary>
    ///     Gets liquidity adjustment based on trade size and asset class.
    /// </summary>
    /// <param name="quantity">The trade quantity.</param>
    /// <param name="assetClass">The asset class.</param>
    /// <returns>Liquidity adjustment factor.</returns>
    private decimal GetLiquidityAdjustment(decimal quantity, string assetClass)
    {
        // Larger trades get less favorable pricing (liquidity impact)
        var liquidityImpact = Math.Min(quantity / 100000m, 0.01m); // Max 1% impact

        return assetClass switch
        {
            FinancialConstants.Equity => liquidityImpact,
            FinancialConstants.FixedIncome => liquidityImpact * 0.5m,
            FinancialConstants.Derivatives => liquidityImpact * 1.5m,
            FinancialConstants.Crypto => liquidityImpact * 2.0m,
            _ => liquidityImpact,
        };
    }

    /// <summary>
    ///     Gets time-based adjustment for intraday price movements.
    /// </summary>
    /// <param name="timestamp">The trade timestamp.</param>
    /// <returns>Time-based adjustment factor.</returns>
    private decimal GetTimeBasedAdjustment(DateTime timestamp)
    {
        // Simulate intraday price movement patterns
        var timeOfDay = timestamp.TimeOfDay;
        var hourProgress = (decimal)(timeOfDay.TotalHours / 24.0);

        // Add some randomness to simulate market microstructure effects
        return (decimal)((_random.NextDouble() - 0.5) * 0.001); // ±0.05% adjustment
    }

    /// <summary>
    ///     Gets validation statistics for monitoring purposes.
    /// </summary>
    /// <returns>A tuple with validation statistics.</returns>
    public (int TotalProcessed, int ValidCount, int InvalidCount, double ValidationRate) GetValidationStats()
    {
        var validationRate = _totalTradesProcessed > 0
            ? (double)_validTrades / _totalTradesProcessed * 100
            : 0;

        return (_totalTradesProcessed, _validTrades, _invalidTrades, validationRate);
    }
}
