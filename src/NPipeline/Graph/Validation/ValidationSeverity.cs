namespace NPipeline.Graph.Validation;

/// <summary>
///     Severity level for a validation issue.
/// </summary>
public enum ValidationSeverity
{
    /// <summary>
    ///     Represents a validation error that prevents successful pipeline execution.
    /// </summary>
    Error,

    /// <summary>
    ///     Represents a warning that does not prevent execution but may indicate a potential issue.
    /// </summary>
    Warning,
}
