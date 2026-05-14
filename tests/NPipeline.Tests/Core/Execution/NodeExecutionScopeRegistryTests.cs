using AwesomeAssertions;
using NPipeline.Execution;
using NPipeline.Observability;

namespace NPipeline.Tests.Core.Execution;

public sealed class NodeExecutionScopeRegistryTests
{
    [Fact]
    public void BeginNodeScope_WithMissingNodeId_ThrowsArgumentException()
    {
        // Arrange
        var registry = new NodeExecutionScopeRegistry();

        // Act
        // Assert
        var act = () => registry.BeginNodeScope(string.Empty);
        _ = act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void BeginNodeScope_WithRegisteredScope_TracksCountsAndDisposes()
    {
        // Arrange
        var registry = new NodeExecutionScopeRegistry();
        var scope = new RecordingScope();
        registry.RegisterNodeObservabilityScope("node-a", scope);

        // Act
        using (var handle = registry.BeginNodeScope("node-a"))
        {
            handle.IncrementProcessed();
            handle.IncrementEmitted();
        }

        // Assert
        _ = scope.Processed.Should().Be(1);
        _ = scope.Emitted.Should().Be(1);
        _ = scope.DisposeCount.Should().Be(1);

        // A second begin after disposal should be a no-op scope.
        using var noOpHandle = registry.BeginNodeScope("node-a");
        noOpHandle.IncrementProcessed();
        noOpHandle.IncrementEmitted();

        _ = scope.Processed.Should().Be(1);
        _ = scope.Emitted.Should().Be(1);
    }

    [Fact]
    public void RecordNodeFailureAndDispose_RecordsFailureAndDisposesScope()
    {
        // Arrange
        var registry = new NodeExecutionScopeRegistry();
        var scope = new RecordingScope();
        var expected = new InvalidOperationException("boom");
        registry.RegisterNodeObservabilityScope("node-fail", scope);

        // Act
        registry.RecordNodeFailureAndDispose("node-fail", expected);

        // Assert
        _ = scope.RecordedFailure.Should().BeSameAs(expected);
        _ = scope.DisposeCount.Should().Be(1);

        // Subsequent failures should be ignored after scope removal.
        registry.RecordNodeFailureAndDispose("node-fail", new Exception("ignored"));
        _ = scope.DisposeCount.Should().Be(1);
    }

    [Fact]
    public void AnnotationsAndRuntimeData_AreManagedThroughRegistry()
    {
        // Arrange
        var registry = new NodeExecutionScopeRegistry();

        // Act
        registry.SetNodeExecutionAnnotation("node-1", 42);
        registry.SetRuntimeAnnotation("diag.resilience.node-1.failures", 3);
        registry.SetRuntimeAnnotation("diag.resilience.node-1.consecutive", 2);
        registry.SetRuntimeAnnotation("parallel.metrics::node-1", new object());

        // Assert annotation storage
        _ = registry.TryGetNodeExecutionAnnotation("node-1", out var annotation).Should().BeTrue();
        _ = annotation.Should().Be(42);
        _ = registry.RemoveNodeExecutionAnnotation("node-1").Should().BeTrue();
        _ = registry.TryGetNodeExecutionAnnotation("node-1", out _).Should().BeFalse();

        // Assert runtime storage and prefix enumeration
        _ = registry.TryGetRuntimeAnnotation("diag.resilience.node-1.failures", out var failures).Should().BeTrue();
        _ = failures.Should().Be(3);

        var diagnostics = registry.EnumerateRuntimeAnnotationsWithPrefix("diag.resilience.").ToList();
        _ = diagnostics.Count.Should().Be(2);
    }

    private sealed class RecordingScope : IAutoObservabilityScope
    {
        public int Processed { get; private set; }
        public int Emitted { get; private set; }
        public int DisposeCount { get; private set; }
        public Exception? RecordedFailure { get; private set; }

        public void RecordItemCount(long processed, long emitted)
        {
            Processed = (int)processed;
            Emitted = (int)emitted;
        }

        public void IncrementProcessed()
        {
            Processed++;
        }

        public void IncrementEmitted()
        {
            Emitted++;
        }

        public void RecordFailure(Exception exception)
        {
            RecordedFailure = exception;
        }

        public Exception? GetFailureException()
        {
            return RecordedFailure;
        }

        public void Dispose()
        {
            DisposeCount++;
        }
    }
}
