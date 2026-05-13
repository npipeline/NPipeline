using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Pipeline;

public sealed class PipelineContextCompositionTests
{
    [Fact]
    public void Constructor_InitializesFocusedContexts()
    {
        // Arrange
        var context = new PipelineContext();

        // Assert
        _ = context.RunIdentity.Should().NotBeNull();
        _ = context.ExecutionConfiguration.Should().NotBeNull();
        _ = context.Observability.Should().NotBeNull();
        _ = context.NodeEnvironment.Should().NotBeNull();
        _ = context.Lineage.Should().NotBeNull();

        _ = context.GlobalRetryOptions.Should().BeSameAs(context.ExecutionConfiguration.GlobalRetryOptions);
        _ = context.RetryOptions.Should().BeSameAs(context.ExecutionConfiguration.RetryOptions);
        _ = context.LoggerFactory.Should().BeSameAs(context.Observability.LoggerFactory);
        _ = context.LineageFactory.Should().BeSameAs(context.Lineage.LineageFactory);
        _ = context.NodeExecutionScopeRegistry.Should().BeSameAs(context.NodeEnvironment.NodeExecutionScopeRegistry);
    }

    [Fact]
    public void LegacyAndFocusedProperties_StayInSync()
    {
        // Arrange
        var context = new PipelineContext(PipelineContextConfiguration.WithRetry(new PipelineRetryOptions(2)));
        var pipelineId = Guid.NewGuid();
        var runId = Guid.NewGuid();
        var effectiveRetryOptions = new PipelineRetryOptions(5);

        // Act
        context.PipelineId = pipelineId;
        context.RunId = runId;
        context.PipelineName = "Orders";
        context.DiOwnedNodes = true;
        context.ExecutionObserver = null!;
        context.GlobalRetryOptions = effectiveRetryOptions;

        using (context.ScopedNode("node-a"))
        {
            _ = context.CurrentNodeId.Should().Be("node-a");
            _ = context.NodeEnvironment.CurrentNodeId.Should().Be("node-a");
        }

        context.RunIdentity.PipelineName = "Invoices";

        // Assert
        _ = context.RunIdentity.PipelineId.Should().Be(pipelineId);
        _ = context.RunIdentity.RunId.Should().Be(runId);
        _ = context.PipelineName.Should().Be("Invoices");
        _ = context.NodeEnvironment.DiOwnedNodes.Should().BeTrue();
        _ = context.ExecutionObserver.Should().BeSameAs(NullExecutionObserver.Instance);
        _ = context.GlobalRetryOptions.Should().BeSameAs(context.ExecutionConfiguration.GlobalRetryOptions);
        _ = context.GlobalRetryOptions.Should().Be(effectiveRetryOptions);
        _ = context.CurrentNodeId.Should().BeEmpty();
    }
}
