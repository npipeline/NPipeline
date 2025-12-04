using System.Collections.Immutable;

namespace NPipeline.Graph.Validation;

/// <summary>
///     Represents the result of pipeline graph validation (categorized + severities).
/// </summary>
public sealed record PipelineValidationResult(ImmutableList<ValidationIssue> Issues)
{
    /// <summary>
    ///     Gets a validation result representing a successful validation with no issues.
    /// </summary>
    public static PipelineValidationResult Success { get; } = new(ImmutableList<ValidationIssue>.Empty);

    /// <summary>
    ///     Gets a value indicating whether the validation passed without any errors.
    /// </summary>
    public bool IsValid => Issues.All(i => i.Severity != ValidationSeverity.Error);

    /// <summary>
    ///     Gets a list of all error messages from the validation issues.
    /// </summary>
    public ImmutableList<string> Errors => Issues.Where(i => i.Severity == ValidationSeverity.Error).Select(i => i.Message).ToImmutableList();

    /// <summary>
    ///     Gets a list of all warning messages from the validation issues.
    /// </summary>
    public ImmutableList<string> Warnings => Issues.Where(i => i.Severity == ValidationSeverity.Warning).Select(i => i.Message).ToImmutableList();
}
