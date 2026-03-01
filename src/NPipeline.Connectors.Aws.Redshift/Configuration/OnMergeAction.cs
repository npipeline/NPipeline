namespace NPipeline.Connectors.Aws.Redshift.Configuration;

/// <summary>Action to take when a matching row is found during an upsert operation.</summary>
public enum OnMergeAction
{
    /// <summary>Update all non-key columns in the target when a match is found.</summary>
    Update,

    /// <summary>Skip matched rows — effectively an insert-only operation.</summary>
    Skip,
}
