using Microsoft.CodeAnalysis;

namespace NPipeline.Analyzers.Tests;

public sealed class TimeoutConfigurationAnalyzerTests
{
    private static async Task<IReadOnlyList<Diagnostic>> AnalyzeAsync(string expression)
    {
        var source = $$"""
                       public static class Setup
                       {
                           public static CircuitBreakerOptions Options(CircuitBreakerOptions existing) => {{expression}};
                       }
                       """;

        var diagnostics = await ResilienceAnalyzerTestHelper.GetDiagnosticsAsync<TimeoutConfigurationAnalyzer>(source);
        return diagnostics.Where(d => d.Id == TimeoutConfigurationAnalyzer.TimeoutConfigurationId).ToList();
    }

    [Theory]
    [InlineData("new CircuitBreakerOptions { OpenDuration = TimeSpan.Zero }", "OpenDuration")]
    [InlineData("new CircuitBreakerOptions { Window = TimeSpan.FromSeconds(-1) }", "Window")]
    [InlineData("existing with { MaxPause = -TimeSpan.FromMinutes(1) }", "MaxPause")]
    public async Task Reports_NonPositiveDuration(string expression, string property)
    {
        var diagnostic = Assert.Single(await AnalyzeAsync(expression));
        Assert.Contains(property, diagnostic.GetMessage());
    }

    [Theory]
    [InlineData(
        "new CircuitBreakerOptions { WhenOpen = BreakerOpenBehavior.Pause, OpenDuration = TimeSpan.FromMinutes(1), MaxPause = TimeSpan.FromSeconds(10) }")]
    [InlineData("new CircuitBreakerOptions { WhenOpen = BreakerOpenBehavior.Pause, MaxPause = TimeSpan.FromSeconds(10) }")]
    [InlineData("new CircuitBreakerOptions { WhenOpen = BreakerOpenBehavior.Pause, OpenDuration = TimeSpan.FromMinutes(10) }")]
    [InlineData("existing with { WhenOpen = BreakerOpenBehavior.Pause, OpenDuration = TimeSpan.FromMinutes(2), MaxPause = TimeSpan.FromMinutes(1) }")]
    public async Task Reports_PauseShorterThanOpenDuration(string expression)
    {
        var diagnostic = Assert.Single(await AnalyzeAsync(expression));
        Assert.Contains("MaxPause", diagnostic.GetMessage());
    }

    [Theory]
    [InlineData("new CircuitBreakerOptions { WhenOpen = BreakerOpenBehavior.Pause }")]
    [InlineData(
        "new CircuitBreakerOptions { WhenOpen = BreakerOpenBehavior.Pause, OpenDuration = TimeSpan.FromSeconds(30), MaxPause = TimeSpan.FromSeconds(30) }")]
    [InlineData("new CircuitBreakerOptions { OpenDuration = TimeSpan.FromMinutes(1), MaxPause = TimeSpan.FromSeconds(10) }")]
    [InlineData("existing with { WhenOpen = BreakerOpenBehavior.Pause, MaxPause = TimeSpan.FromSeconds(1) }")]
    [InlineData("new CircuitBreakerOptions { OpenDuration = TimeSpan.FromMilliseconds(250), Window = TimeSpan.FromHours(1) }")]
    public async Task DoesNotReport_WorkableTimings(string expression) => Assert.Empty(await AnalyzeAsync(expression));
}
