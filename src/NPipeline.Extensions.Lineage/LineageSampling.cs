using NPipeline.Configuration;

namespace NPipeline.Lineage;

/// <summary>
///     The one sampling rule for item-level lineage, shared by the pipeline and <see cref="LineageCollector" /> so they
///     agree on which items are sampled.
/// </summary>
internal static class LineageSampling
{
    /// <summary>
    ///     Whether lineage is collected for the item with <paramref name="correlationId" />.
    /// </summary>
    /// <remarks>
    ///     Deterministic sampling masks the sign bit rather than taking <see cref="Math.Abs(int)" />, which throws for
    ///     <see cref="int.MinValue" />.
    /// </remarks>
    public static bool IsSampled(Guid correlationId, LineageOptions? options)
    {
        if (options is null || options.SampleEvery <= 1)
            return true;

        return options.DeterministicSampling
            ? (correlationId.GetHashCode() & int.MaxValue) % options.SampleEvery == 0
            : Random.Shared.Next(options.SampleEvery) == 0;
    }
}
