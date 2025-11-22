using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_AdvancedAggregateNode.Models;

namespace Sample_AdvancedAggregateNode.Nodes;

/// <summary>
///     Sink node that displays comprehensive risk analytics reports in formatted console output.
///     This node aggregates results from multiple risk calculators and presents them in a unified dashboard.
/// </summary>
/// <remarks>
///     This sink handles multiple result types from different AdvancedAggregateNode implementations:
///     - VolatilityResult: Price volatility analysis by symbol
///     - ValueAtRiskResult: VaR calculations by portfolio
///     - PortfolioAnalyticsResult: Comprehensive portfolio metrics
///     The sink formats and displays these results in a professional risk dashboard format
///     suitable for financial monitoring and analysis.
/// </remarks>
public class RiskReportSink : SinkNode<object>
{
    private readonly List<PortfolioAnalyticsResult> _portfolioResults = new();
    private readonly TimeSpan _reportInterval = TimeSpan.FromSeconds(10);
    private readonly List<ValueAtRiskResult> _varResults = new();
    private readonly List<VolatilityResult> _volatilityResults = new();
    private int _invalidTradesFiltered;
    private DateTime _lastReportTime = DateTime.MinValue;
    private int _totalTradesProcessed;

    /// <summary>
    ///     Initializes a new instance of the RiskReportSink.
    /// </summary>
    public RiskReportSink()
    {
        Console.WriteLine("RiskReportSink: Initialized for comprehensive risk reporting");
        Console.WriteLine("RiskReportSink: Will display formatted risk analytics dashboard");
    }

    /// <summary>
    ///     Processes risk analytics results and displays them in a formatted dashboard.
    /// </summary>
    /// <param name="input">The data pipe containing risk results.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token for async operation.</param>
    /// <returns>A task representing sink operation.</returns>
    public override async Task ExecuteAsync(
        IDataPipe<object> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine("=== FINANCIAL RISK ANALYTICS DASHBOARD ===");
        Console.WriteLine("Real-time risk monitoring and analysis");
        Console.WriteLine();

        await foreach (var result in input.WithCancellation(cancellationToken))
        {
            ProcessResult(result);

            // Generate periodic reports
            if (DateTime.UtcNow - _lastReportTime >= _reportInterval)
            {
                GenerateRiskReport();
                _lastReportTime = DateTime.UtcNow;
            }
        }

        // Final report before shutdown
        GenerateRiskReport();
        Console.WriteLine("RiskReportSink: Risk monitoring completed");
    }

    /// <summary>
    ///     Processes individual results based on their type.
    /// </summary>
    /// <param name="result">The result to process.</param>
    private void ProcessResult(object result)
    {
        switch (result)
        {
            case VolatilityResult volatilityResult:
                ProcessVolatilityResult(volatilityResult);
                break;

            case ValueAtRiskResult varResult:
                ProcessVaRResult(varResult);
                break;

            case PortfolioAnalyticsResult portfolioResult:
                ProcessPortfolioResult(portfolioResult);
                break;

            case ValidatedTrade validatedTrade:
                ProcessValidatedTrade(validatedTrade);
                break;

            default:
                Console.WriteLine($"RiskReportSink: Unknown result type: {result.GetType().Name}");
                break;
        }
    }

    /// <summary>
    ///     Processes volatility calculation results.
    /// </summary>
    /// <param name="result">The volatility result.</param>
    private void ProcessVolatilityResult(VolatilityResult result)
    {
        _volatilityResults.Add(result);

        // Keep only recent results (last 50 per symbol)
        var recentResults = _volatilityResults
            .Where(r => r.Symbol == result.Symbol)
            .OrderByDescending(r => r.WindowEnd)
            .Take(50)
            .ToList();

        // Remove older results for this symbol
        _volatilityResults.RemoveAll(r => r.Symbol == result.Symbol && !recentResults.Contains(r));
        _volatilityResults.AddRange(recentResults.Where(r => !_volatilityResults.Contains(r)));

        Console.WriteLine($"RiskReportSink: Received volatility for {result.Symbol}: " +
                          $"{result.Volatility:P2} ({result.VolatilityPercentage:F2}%)");
    }

    /// <summary>
    ///     Processes Value at Risk calculation results.
    /// </summary>
    /// <param name="result">The VaR result.</param>
    private void ProcessVaRResult(ValueAtRiskResult result)
    {
        _varResults.Add(result);

        // Keep only recent results (last 20 per portfolio)
        var recentResults = _varResults
            .Where(r => r.PortfolioId == result.PortfolioId)
            .OrderByDescending(r => r.WindowEnd)
            .Take(20)
            .ToList();

        // Remove older results for this portfolio
        _varResults.RemoveAll(r => r.PortfolioId == result.PortfolioId && !recentResults.Contains(r));
        _varResults.AddRange(recentResults.Where(r => !_varResults.Contains(r)));

        Console.WriteLine($"RiskReportSink: Received VaR for {result.PortfolioId}: " +
                          $"95%={result.VaR95:C}, 99%={result.VaR99:C}");
    }

    /// <summary>
    ///     Processes portfolio analytics results.
    /// </summary>
    /// <param name="result">The portfolio analytics result.</param>
    private void ProcessPortfolioResult(PortfolioAnalyticsResult result)
    {
        _portfolioResults.Add(result);

        // Keep only recent results (last 20 per portfolio)
        var recentResults = _portfolioResults
            .Where(r => r.PortfolioId == result.PortfolioId)
            .OrderByDescending(r => r.WindowEnd)
            .Take(20)
            .ToList();

        // Remove older results for this portfolio
        _portfolioResults.RemoveAll(r => r.PortfolioId == result.PortfolioId && !recentResults.Contains(r));
        _portfolioResults.AddRange(recentResults.Where(r => !_portfolioResults.Contains(r)));

        var riskLevel = PortfolioAnalyticsCalculator.GetRiskLevel(result.PortfolioVolatility, result.SharpeRatio);

        Console.WriteLine($"RiskReportSink: Received portfolio analytics for {result.PortfolioId}: " +
                          $"return={result.PortfolioReturn:P2}, risk={riskLevel}");
    }

    /// <summary>
    ///     Processes validated trades for statistics tracking.
    /// </summary>
    /// <param name="trade">The validated trade.</param>
    private void ProcessValidatedTrade(ValidatedTrade trade)
    {
        _totalTradesProcessed++;

        if (!trade.IsValid)
            _invalidTradesFiltered++;
    }

    /// <summary>
    ///     Generates a comprehensive risk analytics report.
    /// </summary>
    private void GenerateRiskReport()
    {
        Console.WriteLine();
        Console.WriteLine("================================================================================");
        Console.WriteLine($"                           RISK ANALYTICS REPORT - {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        Console.WriteLine("================================================================================");
        Console.WriteLine();

        // Summary statistics
        DisplaySummaryStatistics();

        // Volatility analysis
        if (_volatilityResults.Count > 0)
            DisplayVolatilityAnalysis();

        // VaR analysis
        if (_varResults.Count > 0)
            DisplayVaRAnalysis();

        // Portfolio analytics
        if (_portfolioResults.Count > 0)
            DisplayPortfolioAnalytics();

        // Risk alerts
        DisplayRiskAlerts();

        Console.WriteLine("================================================================================");
        Console.WriteLine();
    }

    /// <summary>
    ///     Displays summary statistics for the reporting period.
    /// </summary>
    private void DisplaySummaryStatistics()
    {
        Console.WriteLine("SUMMARY STATISTICS");
        Console.WriteLine("------------------");
        Console.WriteLine($"Total Trades Processed: {_totalTradesProcessed:N0}");
        Console.WriteLine($"Invalid Trades Filtered: {_invalidTradesFiltered:N0}");
        Console.WriteLine($"Valid Trades: {_totalTradesProcessed - _invalidTradesFiltered:N0}");

        var validationRate = _totalTradesProcessed > 0
            ? (double)(_totalTradesProcessed - _invalidTradesFiltered) / _totalTradesProcessed * 100
            : 0;

        Console.WriteLine($"Validation Rate: {validationRate:F1}%");
        Console.WriteLine();
    }

    /// <summary>
    ///     Displays volatility analysis by symbol.
    /// </summary>
    private void DisplayVolatilityAnalysis()
    {
        Console.WriteLine("VOLATILITY ANALYSIS");
        Console.WriteLine("---------------------");
        Console.WriteLine("Symbol    | Volatility | Annualized |   Mean   |   Range   | Trades");
        Console.WriteLine("-----------|------------|------------|-----------|-----------|-------");

        var latestVolatility = _volatilityResults
            .GroupBy(r => r.Symbol)
            .Select(g => g.OrderByDescending(r => r.WindowEnd).First())
            .OrderByDescending(r => r.Volatility);

        foreach (var vol in latestVolatility.Take(10))
        {
            Console.WriteLine($"{vol.Symbol,-10} | {vol.Volatility:P2,-10} | {vol.AnnualizedVolatility:P2,-10} | " +
                              $"{vol.MeanPrice:F2,-9} | {vol.PriceRange:F2,-9} | {vol.TradeCount,5}");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Displays Value at Risk analysis by portfolio.
    /// </summary>
    private void DisplayVaRAnalysis()
    {
        Console.WriteLine("VALUE AT RISK (VaR) ANALYSIS");
        Console.WriteLine("------------------------------");
        Console.WriteLine("Portfolio               |  95% VaR  |  99% VaR  |   ES95    |   ES99    | Samples");
        Console.WriteLine("------------------------|------------|------------|-----------|-----------|--------");

        var latestVaR = _varResults
            .GroupBy(r => r.PortfolioId)
            .Select(g => g.OrderByDescending(r => r.WindowEnd).First())
            .OrderByDescending(r => r.VaR95);

        foreach (var var in latestVaR.Take(8))
        {
            var portfolioId = var.PortfolioId.Length > 22
                ? var.PortfolioId.Substring(0, 22)
                : var.PortfolioId;

            Console.WriteLine($"{portfolioId,-23} | {var.VaR95:C,-10} | {var.VaR99:C,-10} | " +
                              $"{var.ExpectedShortfall95:C,-9} | {var.ExpectedShortfall99:C,-9} | {var.SampleSize,7}");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Displays portfolio analytics with risk metrics.
    /// </summary>
    private void DisplayPortfolioAnalytics()
    {
        Console.WriteLine("PORTFOLIO ANALYTICS");
        Console.WriteLine("--------------------");
        Console.WriteLine("Portfolio               |   Value   |  Return   | Volatility | Sharpe | Assets | Trades | Risk Level");
        Console.WriteLine("------------------------|-----------|-----------|------------|--------|--------|--------|------------");

        var latestPortfolio = _portfolioResults
            .GroupBy(r => r.PortfolioId)
            .Select(g => g.OrderByDescending(r => r.WindowEnd).First())
            .OrderByDescending(r => r.TotalValue);

        foreach (var portfolio in latestPortfolio.Take(8))
        {
            var portfolioId = portfolio.PortfolioId.Length > 22
                ? portfolio.PortfolioId.Substring(0, 22)
                : portfolio.PortfolioId;

            var riskLevel = PortfolioAnalyticsCalculator.GetRiskLevel(portfolio.PortfolioVolatility, portfolio.SharpeRatio);
            var diversification = PortfolioAnalyticsCalculator.CalculateDiversificationScore(portfolio.AssetWeights);

            Console.WriteLine($"{portfolioId,-23} | {portfolio.TotalValue:C,-9} | {portfolio.PortfolioReturn:P2,-9} | " +
                              $"{portfolio.PortfolioVolatility:P2,-10} | {portfolio.SharpeRatio:F2,-6} | " +
                              $"{portfolio.AssetCount,6} | {portfolio.TradeCount,6} | {riskLevel,-11}");
        }

        Console.WriteLine();
    }

    /// <summary>
    ///     Displays risk alerts based on current metrics.
    /// </summary>
    private void DisplayRiskAlerts()
    {
        Console.WriteLine("RISK ALERTS");
        Console.WriteLine("-----------");

        var alerts = new List<string>();

        // High volatility alerts
        var highVolatility = _volatilityResults
            .Where(r => r.Volatility > 0.03m) // > 3% daily volatility
            .GroupBy(r => r.Symbol)
            .Select(g => g.OrderByDescending(r => r.WindowEnd).First())
            .ToList();

        foreach (var vol in highVolatility.Take(3))
        {
            alerts.Add($"🔴 HIGH VOLATILITY: {vol.Symbol} at {vol.Volatility:P2} ({vol.VolatilityPercentage:F1}%)");
        }

        // High VaR alerts
        var highVaR = _varResults
            .Where(r => r.VaR95 > 1000000m) // > $1M VaR
            .GroupBy(r => r.PortfolioId)
            .Select(g => g.OrderByDescending(r => r.WindowEnd).First())
            .ToList();

        foreach (var var in highVaR.Take(3))
        {
            alerts.Add($"🟠 HIGH VaR: {var.PortfolioId} 95% VaR at {var.VaR95:C}");
        }

        // Low Sharpe ratio alerts
        var lowSharpe = _portfolioResults
            .Where(r => r.SharpeRatio < 0.5m)
            .GroupBy(r => r.PortfolioId)
            .Select(g => g.OrderByDescending(r => r.WindowEnd).First())
            .ToList();

        foreach (var portfolio in lowSharpe.Take(3))
        {
            alerts.Add($"🟡 LOW RISK-ADJUSTED RETURN: {portfolio.PortfolioId} Sharpe ratio {portfolio.SharpeRatio:F2}");
        }

        if (alerts.Count > 0)
        {
            foreach (var alert in alerts.Take(5))
            {
                Console.WriteLine(alert);
            }
        }
        else
            Console.WriteLine("✅ No significant risk alerts detected");

        Console.WriteLine();
    }

    /// <summary>
    ///     Gets current statistics for monitoring purposes.
    /// </summary>
    /// <returns>A tuple with processing statistics.</returns>
    public (int TotalTrades, int InvalidTrades, int VolatilityResults, int VaRResults, int PortfolioResults) GetStatistics()
    {
        return (
            _totalTradesProcessed,
            _invalidTradesFiltered,
            _volatilityResults.Count,
            _varResults.Count,
            _portfolioResults.Count
        );
    }
}
