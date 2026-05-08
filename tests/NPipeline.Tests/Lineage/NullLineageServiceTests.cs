using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Execution.Services;
using NPipeline.Lineage;

namespace NPipeline.Tests.Lineage;

public sealed class NullLineageServiceTests
{
    [Fact]
    public void NullLineageService_Instance_IsNotNull()
    {
        Assert.NotNull(NullLineageService.Instance);
    }

    [Fact]
    public void NullLineageService_WrapSourceStream_ReturnsOriginalStream()
    {
        var service = NullLineageService.Instance;
        var source = new DataStream<int>(Array.Empty<int>().ToAsyncEnumerable(), "test");
        var result = service.WrapSourceStream(source, "n1", Guid.NewGuid(), "pipeline", null);
        Assert.Same(source, result);
    }

    [Fact]
    public async Task NullLineageService_UnwrapLineageStream_ReturnsItemsUnchanged()
    {
        var service = NullLineageService.Instance;
        var items = new object[] { 1, "hello", null! }.ToAsyncEnumerable();
        var results = new List<object?>();
        await foreach (var item in service.UnwrapLineageStream(items))
            results.Add(item);
        Assert.Equal(3, results.Count);
        Assert.Equal(1, results[0]);
        Assert.Equal("hello", results[1]);
        Assert.Null(results[2]);
    }

    [Fact]
    public void NullLineageService_PrepareInputWithLineageContext_ReturnsOriginalInput()
    {
        var service = NullLineageService.Instance;
        var source = new DataStream<int>(Array.Empty<int>().ToAsyncEnumerable(), "test");
        var (unwrapped, _) = service.PrepareInputWithLineageContext(source);
        Assert.Same(source, unwrapped);
    }

    [Fact]
    public void NullLineageService_WrapNodeOutput_ReturnsOriginalStream()
    {
        var service = NullLineageService.Instance;
        var output = new DataStream<string>(Array.Empty<string>().ToAsyncEnumerable(), "out");
        var result = service.WrapNodeOutput(output, "n1", Guid.NewGuid(), null, null, LineageOutcomeReason.Emitted);
        Assert.Same(output, result);
    }

    [Fact]
    public void NullLineageService_WrapNodeOutputFromInputLineage_ReturnsOriginalStream()
    {
        var service = NullLineageService.Instance;
        var output = new DataStream<string>(Array.Empty<string>().ToAsyncEnumerable(), "out");
        var context = Array.Empty<object>().ToAsyncEnumerable();
        var result = service.WrapNodeOutputFromInputLineage(output, context, "n1", Guid.NewGuid(), null, null, LineageOutcomeReason.Joined);
        Assert.Same(output, result);
    }
}
