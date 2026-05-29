using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Shared helper for reading the NPipelineOptimizationProfile MSBuild property
///     from analyzer configuration options.
/// </summary>
internal static class AnalyzerProfileHelper
{
    private const string PropertyName = "build_property.NPipelineOptimizationProfile";

    /// <summary>
    ///     Returns true if the HighThroughput profile is enabled, meaning performance-sensitive
    ///     analyzers should register and fire.
    /// </summary>
    public static bool IsHighThroughput(AnalyzerOptions options, Compilation compilation)
    {
        if (options.AnalyzerConfigOptionsProvider.GlobalOptions.TryGetValue(PropertyName, out var globalProfile))
            return IsHighThroughputProfile(globalProfile);

        foreach (var tree in compilation.SyntaxTrees)
        {
            var configOptions = options.AnalyzerConfigOptionsProvider.GetOptions(tree);
            if (configOptions.TryGetValue(PropertyName, out var treeProfile))
                return IsHighThroughputProfile(treeProfile);
        }

        return false;
    }

    private static bool IsHighThroughputProfile(string? profile)
    {
        return string.Equals(profile?.Trim(), "HighThroughput", System.StringComparison.OrdinalIgnoreCase);
    }
}
