namespace NPipeline.Graph.Validation;

/// <summary>
///     A single validation issue (error or warning) with optional category and detail.
/// </summary>
public sealed record ValidationIssue(ValidationSeverity Severity, string Message, string Category)
{
    /// <summary>
    ///     Returns a string representation of the validation issue in the format "[Severity] Category: Message".
    /// </summary>
    /// <returns>A formatted string containing the severity, category, and message of the validation issue.</returns>
    public override string ToString()
    {
        return $"[{Severity}] {Category}: {Message}";
    }
}
