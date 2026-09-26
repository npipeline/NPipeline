namespace NPipeline.Tests.Core.Execution;

/// <summary>
///     Groups tests that assert on process-wide counters. The collection disables parallelization so no other test can
///     mutate those counters while they run.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ProcessWideCounterGroup
{
    public const string Name = "ProcessWideCounters";
}
