namespace NPipeline.Lineage;

/// <summary>
///     Lineage sink decorator that tees every record into an <see cref="ILineageCollector" />
///     before forwarding it to the configured sink (when one is present).
/// </summary>
/// <remarks>
///     <para>
///         Item-level lineage records are emitted through a single funnel (the sink-unwrap stage at each
///         terminal node). This decorator hooks that funnel so a registered collector observes the same
///         records the sink does, which is what makes the collector's query surface
///         (<see cref="ILineageCollector.GetAllRecords" />, <see cref="ILineageCollector.GetCorrelationHistory" />,
///         <see cref="ILineageCollector.GetUnresolvedCorrelations" />) meaningful for a run.
///     </para>
///     <para>
///         The inner sink is optional: a collector alone is enough to make the pipeline emit records, so a
///         caller who registers a collector without a sink still gets a populated collector.
///     </para>
///     <para>
///         The collector retains every record it is given for the lifetime of the run, so memory grows with
///         the number of tracked items. Prefer sampling via <see cref="NPipeline.Configuration.LineageOptions" />
///         on high-volume pipelines.
///     </para>
/// </remarks>
internal sealed class CollectorTeeingLineageSink : ILineageSink
{
    private readonly ILineageCollector _collector;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CollectorTeeingLineageSink" /> class.
    /// </summary>
    /// <param name="collector">The collector that receives every record.</param>
    /// <param name="inner">The configured sink to forward to, or null when none is configured.</param>
    public CollectorTeeingLineageSink(ILineageCollector collector, ILineageSink? inner)
    {
        _collector = collector ?? throw new ArgumentNullException(nameof(collector));
        Inner = inner;
    }

    /// <summary>
    ///     Gets the sink this decorator forwards to, if any.
    /// </summary>
    public ILineageSink? Inner { get; }

    /// <inheritdoc />
    public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken)
    {
        _collector.Record(record);

        return Inner is null
            ? Task.CompletedTask
            : Inner.RecordAsync(record, cancellationToken);
    }
}
