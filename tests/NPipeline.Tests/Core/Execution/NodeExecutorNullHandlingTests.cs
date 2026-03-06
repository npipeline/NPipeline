using AwesomeAssertions;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution.Services;

namespace NPipeline.Tests.Core.Execution;

public sealed class NodeExecutorNullHandlingTests
{
    [Fact]
    public async Task BuildOutputAdapter_WithReferenceTypeNullItems_YieldsNull()
    {
        var adapter = NodeInstantiationService.BuildOutputAdapter(typeof(string))!;
        var input = new List<object?> { "value1", null, "value3" }.ToAsyncEnumerable();
        var inputPipe = new StreamingDataPipe<object?>(input);

        var resultPipe = adapter(inputPipe, "test-stream");
        var results = new List<string?>();

        await foreach (var item in resultPipe.ToAsyncEnumerable())
        {
            results.Add((string?)item);
        }

        // Assert
        results.Should().HaveCount(3);
        results[0].Should().Be("value1");
        results[1].Should().BeNull();
        results[2].Should().Be("value3");
    }

    [Fact]
    public async Task BuildOutputAdapter_WithValueTypeNullItems_YieldsDefault()
    {
        var adapter = NodeInstantiationService.BuildOutputAdapter(typeof(int))!;
        var input = new List<object?> { 1, null, 3 }.ToAsyncEnumerable();
        var inputPipe = new StreamingDataPipe<object?>(input);

        var resultPipe = adapter(inputPipe, "test-stream");
        var results = new List<int>();

        await foreach (var item in resultPipe.ToAsyncEnumerable())
        {
            results.Add((int)item!);
        }

        // Assert
        results.Should().HaveCount(3);
        results[0].Should().Be(1);
        results[1].Should().Be(0); // default for int
        results[2].Should().Be(3);
    }

    [Fact]
    public async Task BuildOutputAdapter_WithNonNullableValueTypeNullItems_YieldsDefault()
    {
        var adapter = NodeInstantiationService.BuildOutputAdapter(typeof(int))!;
        var input = new List<object?> { 1, null, 3 }.ToAsyncEnumerable();
        var inputPipe = new StreamingDataPipe<object?>(input);

        var resultPipe = adapter(inputPipe, "test-stream");
        var results = new List<int>();

        await foreach (var item in resultPipe.ToAsyncEnumerable())
        {
            results.Add((int)item!);
        }

        // Assert
        results.Should().HaveCount(3);
        results[0].Should().Be(1);
        results[1].Should().Be(0); // default for int
        results[2].Should().Be(3);
    }

    [Fact]
    public async Task BuildOutputAdapter_WithReferenceTypeTypedPipe_YieldsNull()
    {
        var adapter = NodeInstantiationService.BuildOutputAdapter(typeof(string))!;
        var input = new List<string?> { "value1", null, "value3" }.ToAsyncEnumerable();
        var inputPipe = new StreamingDataPipe<string?>(input);

        var resultPipe = adapter(inputPipe, "test-stream");
        var results = new List<string?>();

        await foreach (var item in resultPipe.ToAsyncEnumerable())
        {
            results.Add((string?)item);
        }

        // Assert
        results.Should().HaveCount(3);
        results[0].Should().Be("value1");
        results[1].Should().BeNull();
        results[2].Should().Be("value3");
    }

    [Fact]
    public async Task BuildOutputAdapter_WithValueTypeTypedPipe_YieldsDefault()
    {
        var adapter = NodeInstantiationService.BuildOutputAdapter(typeof(int))!;
        var input = new List<int?> { 1, null, 3 }.ToAsyncEnumerable();
        var inputPipe = new StreamingDataPipe<int?>(input);

        var resultPipe = adapter(inputPipe, "test-stream");
        var results = new List<int>();

        await foreach (var item in resultPipe.ToAsyncEnumerable())
        {
            results.Add((int)item!);
        }

        // Assert
        results.Should().HaveCount(3);
        results[0].Should().Be(1);
        results[1].Should().Be(0); // default for int
        results[2].Should().Be(3);
    }

    [Fact]
    public async Task BuildOutputAdapter_WithNonNullableValueTypeTypedPipe_YieldsDefault()
    {
        var adapter = NodeInstantiationService.BuildOutputAdapter(typeof(int))!;
        var input = new List<int?> { 1, null, 3 }.ToAsyncEnumerable();
        var inputPipe = new StreamingDataPipe<int?>(input);

        var resultPipe = adapter(inputPipe, "test-stream");
        var results = new List<int>();

        await foreach (var item in resultPipe.ToAsyncEnumerable())
        {
            results.Add((int)item!);
        }

        // Assert
        results.Should().HaveCount(3);
        results[0].Should().Be(1);
        results[1].Should().Be(0); // default for int
        results[2].Should().Be(3);
    }

    [Fact]
    public async Task BuildOutputAdapter_WithBoolNullItems_YieldsDefaultFalse()
    {
        var adapter = NodeInstantiationService.BuildOutputAdapter(typeof(bool))!;
        var input = new List<object?> { true, null, false }.ToAsyncEnumerable();
        var inputPipe = new StreamingDataPipe<object?>(input);

        var resultPipe = adapter(inputPipe, "test-stream");
        var results = new List<bool>();

        await foreach (var item in resultPipe.ToAsyncEnumerable())
        {
            results.Add((bool)item!);
        }

        // Assert
        results.Should().HaveCount(3);
        results[0].Should().BeTrue();
        results[1].Should().BeFalse(); // default for bool
        results[2].Should().BeFalse();
    }

    [Fact]
    public async Task BuildOutputAdapter_WithBoolTypedPipeNullItems_YieldsDefaultFalse()
    {
        var adapter = NodeInstantiationService.BuildOutputAdapter(typeof(bool))!;
        var input = new List<bool?> { true, null, false }.ToAsyncEnumerable();
        var inputPipe = new StreamingDataPipe<bool?>(input);

        var resultPipe = adapter(inputPipe, "test-stream");
        var results = new List<bool>();

        await foreach (var item in resultPipe.ToAsyncEnumerable())
        {
            results.Add((bool)item!);
        }

        // Assert
        results.Should().HaveCount(3);
        results[0].Should().BeTrue();
        results[1].Should().BeFalse(); // default for bool
        results[2].Should().BeFalse();
    }
}
