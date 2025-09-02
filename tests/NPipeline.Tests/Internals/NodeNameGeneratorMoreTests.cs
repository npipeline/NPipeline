using AwesomeAssertions;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Pipeline;
using NPipeline.Pipeline.Internals;

namespace NPipeline.Tests.Internals;

public sealed class NodeNameGeneratorMoreTests
{
    [Fact]
    public void GenerateIdFromName_AppendsSuffix_WhenCollision()
    {
        var builder = new PipelineBuilder();
        var a = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "dup", [1]);

        // Simulate existing dictionary with key "dup"
        var existing = new Dictionary<string, NodeDefinition> { { a.Id, new NodeDefinition(a.Id, "dup", typeof(object), NodeKind.Source) } };

        var id = NodeNameGenerator.GenerateIdFromName("dup", existing);

        id.Should().StartWith("dup-");
    }

    [Fact]
    public void GenerateUniqueNodeName_IsCaseInsensitive()
    {
        var builder = new PipelineBuilder();
        var a = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "NodeX", [1]);
        var list = new[] { new NodeDefinition(a.Id, "NodeX", typeof(object), NodeKind.Source) };

        var unique = NodeNameGenerator.GenerateUniqueNodeName("nodex", list);

        unique.Should().StartWith("nodex-");
    }

    [Fact]
    public void EnsureUniqueName_ThrowsOnDuplicate_CaseInsensitive()
    {
        var builder = new PipelineBuilder();
        var a = builder.AddInMemorySourceWithDataFromContext(PipelineContext.Default, "MyNode", [1]);
        var list = new[] { new NodeDefinition(a.Id, "MyNode", typeof(object), NodeKind.Source) };

        var act = () => NodeNameGenerator.EnsureUniqueName("mynode", list);

        act.Should().Throw<ArgumentException>().WithMessage("*already been added*");
    }
}
