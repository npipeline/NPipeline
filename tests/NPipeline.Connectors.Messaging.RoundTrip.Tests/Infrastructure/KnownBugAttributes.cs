namespace NPipeline.Connectors.Messaging.RoundTrip.Tests.Infrastructure;

/// <summary>
///     Marks a test that asserts correct behaviour but fails today because of a bug from the connector review. The test
///     is skipped unless <c>NPIPELINE_RUN_KNOWN_BUGS=1</c> is set, so CI stays green while the bug is open. When the bug
///     is fixed, replace this attribute with <see cref="FactAttribute" />.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class KnownBugFactAttribute : FactAttribute
{
    public KnownBugFactAttribute(string bugId)
    {
        BugId = bugId;

        if (!KnownBugs.Run)
            Skip = KnownBugs.SkipReason(bugId);
    }

    /// <summary>The bug's ID in the connector review, for example <c>CSV-1</c>.</summary>
    public string BugId { get; }
}

/// <summary>The <see cref="TheoryAttribute" /> counterpart of <see cref="KnownBugFactAttribute" />.</summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class KnownBugTheoryAttribute : TheoryAttribute
{
    public KnownBugTheoryAttribute(string bugId)
    {
        BugId = bugId;

        if (!KnownBugs.Run)
            Skip = KnownBugs.SkipReason(bugId);
    }

    public string BugId { get; }
}

internal static class KnownBugs
{
    public const string EnvironmentVariable = "NPIPELINE_RUN_KNOWN_BUGS";

    public static bool Run { get; } = Environment.GetEnvironmentVariable(EnvironmentVariable) is "1" or "true";

    public static string SkipReason(string bugId) =>
        $"Known bug {bugId} from the connector review. Set {EnvironmentVariable}=1 to run it.";
}
