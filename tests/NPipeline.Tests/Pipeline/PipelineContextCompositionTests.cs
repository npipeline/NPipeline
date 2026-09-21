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

        _ = context.ExecutionConfiguration.GlobalRetryOptions.Should().BeSameAs(context.ExecutionConfiguration.GlobalRetryOptions);
        _ = context.ExecutionConfiguration.RetryOptions.Should().BeSameAs(context.ExecutionConfiguration.RetryOptions);
        _ = context.Observability.LoggerFactory.Should().BeSameAs(context.Observability.LoggerFactory);
        _ = context.Lineage.LineageFactory.Should().BeSameAs(context.Lineage.LineageFactory);
        _ = context.NodeEnvironment.NodeExecutionScopeRegistry.Should().BeSameAs(context.NodeEnvironment.NodeExecutionScopeRegistry);
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
        context.RunIdentity.PipelineId = pipelineId;
        context.RunIdentity.RunId = runId;
        context.RunIdentity.PipelineName = "Orders";
        context.NodeEnvironment.DiOwnedNodes = true;
        context.Observability.ExecutionObserver = null!;
        context.ExecutionConfiguration.GlobalRetryOptions = effectiveRetryOptions;

        context.RunIdentity.PipelineName = "Invoices";

        // Assert
        _ = context.RunIdentity.PipelineId.Should().Be(pipelineId);
        _ = context.RunIdentity.RunId.Should().Be(runId);
        _ = context.RunIdentity.PipelineName.Should().Be("Invoices");
        _ = context.NodeEnvironment.DiOwnedNodes.Should().BeTrue();
        _ = context.Observability.ExecutionObserver.Should().BeSameAs(NullExecutionObserver.Instance);
        _ = context.ExecutionConfiguration.GlobalRetryOptions.Should().BeSameAs(context.ExecutionConfiguration.GlobalRetryOptions);
        _ = context.ExecutionConfiguration.GlobalRetryOptions.Should().Be(effectiveRetryOptions);
    }
}
