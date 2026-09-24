using AwesomeAssertions;
using NPipeline.Execution;
using NPipeline.Pipeline;
using NPipeline.Reliability;

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

        _ = context.ExecutionConfiguration.Resilience.Should().BeSameAs(PipelineResilienceOptions.None);
        _ = context.Observability.LoggerFactory.Should().BeSameAs(context.Observability.LoggerFactory);
        _ = context.Lineage.LineageFactory.Should().BeSameAs(context.Lineage.LineageFactory);
        _ = context.NodeEnvironment.NodeExecutionScopeRegistry.Should().BeSameAs(context.NodeEnvironment.NodeExecutionScopeRegistry);
    }

    [Fact]
    public void LegacyAndFocusedProperties_StayInSync()
    {
        // Arrange
        var context = new PipelineContext();
        var pipelineId = Guid.NewGuid();
        var runId = Guid.NewGuid();
        var resilience = PipelineResilienceOptions.None with { ItemRetry = new ItemRetryOptions { MaxRetries = 5 } };

        // Act
        context.RunIdentity.PipelineId = pipelineId;
        context.RunIdentity.RunId = runId;
        context.RunIdentity.PipelineName = "Orders";
        context.NodeEnvironment.DiOwnedNodes = true;
        context.Observability.ExecutionObserver = null!;
        context.ExecutionConfiguration.Resilience = resilience;
        context.ExecutionConfiguration.SetNodeResilienceOptions("node", PipelineResilienceOptions.None);

        context.RunIdentity.PipelineName = "Invoices";

        // Assert
        _ = context.RunIdentity.PipelineId.Should().Be(pipelineId);
        _ = context.RunIdentity.RunId.Should().Be(runId);
        _ = context.RunIdentity.PipelineName.Should().Be("Invoices");
        _ = context.NodeEnvironment.DiOwnedNodes.Should().BeTrue();
        _ = context.Observability.ExecutionObserver.Should().BeSameAs(NullExecutionObserver.Instance);
        _ = context.ExecutionConfiguration.Resilience.Should().BeSameAs(resilience);
        _ = context.ExecutionConfiguration.GetResilienceOptions("other").Should().BeSameAs(resilience);
        _ = context.ExecutionConfiguration.GetResilienceOptions("node").Should().BeSameAs(PipelineResilienceOptions.None);
    }
}
