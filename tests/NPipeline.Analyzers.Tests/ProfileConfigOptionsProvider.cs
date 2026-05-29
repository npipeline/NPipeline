using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Pre-built <see cref="AnalyzerOptions" /> that sets the optimization profile to HighThroughput.
/// </summary>
internal static class TestAnalyzerOptions
{
    private const string ProfilePropertyKey = "build_property.NPipelineOptimizationProfile";

    /// <summary>
    ///     Analyzer options with the optimization profile set to HighThroughput,
    ///     enabling all performance-sensitive analyzers.
    /// </summary>
    public static readonly AnalyzerOptions HighThroughput = new(
        ImmutableArray<AdditionalText>.Empty,
        new ConfigOptionsProvider(ProfilePropertyKey, "HighThroughput"));

    /// <summary>
    ///     Analyzer options with the optimization profile set to Default,
    ///     suppressing performance-sensitive analyzers (NP9103–NP9107).
    /// </summary>
    public static readonly AnalyzerOptions Default = new(
        ImmutableArray<AdditionalText>.Empty,
        new ConfigOptionsProvider(ProfilePropertyKey, "Default"));

    /// <summary>
    ///     Analyzer options with no optimization profile set,
    ///     behaving as Default (the MSBuild default).
    /// </summary>
    public static readonly AnalyzerOptions Unset = new(
        ImmutableArray<AdditionalText>.Empty,
        new ConfigOptionsProvider(ProfilePropertyKey, null));
}

/// <summary>
///     A minimal <see cref="AnalyzerConfigOptionsProvider" /> that serves a single key-value pair.
/// </summary>
internal sealed class ConfigOptionsProvider : AnalyzerConfigOptionsProvider
{
    private readonly SingleValueConfigOptions _options;

    public ConfigOptionsProvider(string key, string? value)
    {
        _options = new SingleValueConfigOptions(key, value);
    }

    public override AnalyzerConfigOptions GlobalOptions => _options;

    public override AnalyzerConfigOptions GetOptions(SyntaxTree tree) => _options;

    public override AnalyzerConfigOptions GetOptions(AdditionalText textFile) => _options;
}

/// <summary>
///     An <see cref="AnalyzerConfigOptions" /> that serves a single key-value pair.
/// </summary>
internal sealed class SingleValueConfigOptions : AnalyzerConfigOptions
{
    private readonly string _key;
    private readonly string? _value;

    public SingleValueConfigOptions(string key, string? value)
    {
        _key = key;
        _value = value;
    }

    public override bool TryGetValue(string key, out string value)
    {
        if (key == _key && _value != null)
        {
            value = _value;
            return true;
        }

        value = string.Empty;
        return false;
    }
}
