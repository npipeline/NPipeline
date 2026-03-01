namespace NPipeline.Connectors.Aws.Redshift.Configuration;

/// <summary>
///     Redshift table distribution style hints used when creating staging tables
///     for the upsert pattern. Does not alter existing tables, but ensures
///     staging tables are co-located for optimal DELETE + INSERT performance.
/// </summary>
public enum RedshiftDistributionStyle
{
    /// <summary>Redshift chooses the style automatically (default).</summary>
    Auto,

    /// <summary>Distribute all rows to a single node slice. Good for small lookup tables.</summary>
    All,

    /// <summary>Distribute rows evenly across slices using a round-robin approach.</summary>
    Even,

    /// <summary>Distribute rows based on a distribution key column.</summary>
    Key,
}
