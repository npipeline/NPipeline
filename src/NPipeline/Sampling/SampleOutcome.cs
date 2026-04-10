namespace NPipeline.Sampling;

/// <summary>
/// Processing outcome for a sampled record.
/// </summary>
public enum SampleOutcome
{
    /// <summary>
    /// The item completed processing successfully.
    /// </summary>
    Success,

    /// <summary>
    /// The item ended in an error outcome.
    /// </summary>
    Error,

    /// <summary>
    /// The item was routed to a dead-letter destination.
    /// </summary>
    DeadLetter,

    /// <summary>
    /// The item was skipped or filtered out.
    /// </summary>
    Skipped,
}
