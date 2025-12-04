using System.Collections.Immutable;
using AwesomeAssertions;
using NPipeline.Graph;
using NPipeline.Graph.Validation;

namespace NPipeline.Tests.Validation.BuilderRules;

public sealed class ValidationResultHelpersTests
{
    [Fact]
    public void ValidationIssue_ToString_IncludesSeverityAndCategory()
    {
        // Arrange
        var issue = new ValidationIssue(ValidationSeverity.Error, "Something bad", "Cat");

        // Act
        var text = issue.ToString();

        // Assert
        text.Should().Contain("[Error]").And.Contain("Cat: Something bad");
    }

    [Fact]
    public void PipelineValidationResult_WithMixedIssues_ReportsValidStatusAndIssuesCorrectly()
    {
        // Arrange
        var issues = ImmutableList.Create(
            new ValidationIssue(ValidationSeverity.Error, "e1", "c"),
            new ValidationIssue(ValidationSeverity.Warning, "w1", "c2"));

        // Act
        var result = new PipelineValidationResult(issues);

        // Assert
        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain("e1");
        result.Warnings.Should().Contain("w1");
    }
}
