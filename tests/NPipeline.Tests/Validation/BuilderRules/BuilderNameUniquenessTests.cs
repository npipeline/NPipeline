using AwesomeAssertions;
using NPipeline.Extensions.Testing;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Validation.BuilderRules;

public sealed class BuilderNameUniquenessTests
{
    [Fact(DisplayName = "AddNode with duplicate name should throw (early validation)")]
    public void AddNode_WithDuplicateName_Throws()
    {
        var builder = new PipelineBuilder();
        builder.WithEarlyNameValidation();

        builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "dup", [1]);

        // Adding another source with the same name should trigger early name validation
        Action act = () => builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "dup", [2]);

        act.Should()
            .Throw<ArgumentException>()
            .WithMessage("*already been added*");
    }

    [Fact(DisplayName = "AddNode with case-insensitive duplicate name should throw")]
    public void AddNode_WithCaseInsensitiveDuplicateName_Throws()
    {
        var builder = new PipelineBuilder();
        builder.WithEarlyNameValidation();

        builder.AddInMemorySink<int>("Out");

        // Case-insensitive duplicate
        Action act = () => builder.AddInMemorySink<int>("out");

        act.Should()
            .Throw<ArgumentException>();
    }

    // Minimal test nodes

    // Removed custom dummy nodes in favor of InMemorySourceNode / InMemorySinkNode helpers.
}
