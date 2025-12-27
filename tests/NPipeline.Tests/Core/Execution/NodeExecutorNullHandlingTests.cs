using System.Reflection;
using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution.Services;

namespace NPipeline.Tests.Core.Execution;

public sealed class NodeExecutorNullHandlingTests
{
    [Fact]
    public async Task CreateTypedJoinPipeGeneric_WithReferenceTypeNullItems_YieldsNull()
    {
        // Arrange
        var method = typeof(NodeExecutor).GetMethod("CreateTypedJoinPipeGeneric", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(string));

        var input = new List<object?> { "value1", null, "value3" }.ToAsyncEnumerable();

        // Act
        var resultPipe = (IDataPipe)method.Invoke(null, [input, "test-stream"])!;
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
    public async Task CreateTypedJoinPipeGeneric_WithValueTypeNullItems_YieldsDefault()
    {
        // Arrange
        var method = typeof(NodeExecutor).GetMethod("CreateTypedJoinPipeGeneric", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(int));

        var input = new List<object?> { 1, null, 3 }.ToAsyncEnumerable();

        // Act
        var resultPipe = (IDataPipe)method.Invoke(null, [input, "test-stream"])!;
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
    public async Task CreateTypedJoinPipeGeneric_WithNonNullableValueTypeNullItems_YieldsDefault()
    {
        // Arrange
        var method = typeof(NodeExecutor).GetMethod("CreateTypedJoinPipeGeneric", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(int));

        var input = new List<object?> { 1, null, 3 }.ToAsyncEnumerable();

        // Act
        var resultPipe = (IDataPipe)method.Invoke(null, [input, "test-stream"])!;
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
    public async Task AdaptJoinOutput_WithReferenceTypeNullItems_YieldsNull()
    {
        // Arrange
        var method = typeof(NodeExecutor).GetMethod("AdaptJoinOutput", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(string));

        var input = new List<string?> { "value1", null, "value3" }.ToAsyncEnumerable();
        var inputPipe = new StreamingDataPipe<object?>(input);

        // Act
        var resultPipe = (IDataPipe)method.Invoke(null, [inputPipe, "test-stream"])!;
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
    public async Task AdaptJoinOutput_WithValueTypeNullItems_YieldsDefault()
    {
        // Arrange
        var method = typeof(NodeExecutor).GetMethod("AdaptJoinOutput", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(int));

        var input = new List<object?> { 1, null, 3 }.ToAsyncEnumerable();
        var inputPipe = new StreamingDataPipe<object?>(input);

        // Act
        var resultPipe = (IDataPipe)method.Invoke(null, [inputPipe, "test-stream"])!;
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
    public async Task AdaptJoinOutput_WithNonNullableValueTypeNullItems_YieldsDefault()
    {
        // Arrange
        var method = typeof(NodeExecutor).GetMethod("AdaptJoinOutput", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(int));

        var input = new List<object?> { 1, null, 3 }.ToAsyncEnumerable();
        var inputPipe = new StreamingDataPipe<object?>(input);

        // Act
        var resultPipe = (IDataPipe)method.Invoke(null, [inputPipe, "test-stream"])!;
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
    public async Task CreateTypedJoinPipeGeneric_WithBoolNullItems_YieldsDefaultFalse()
    {
        // Arrange
        var method = typeof(NodeExecutor).GetMethod("CreateTypedJoinPipeGeneric", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(bool));

        var input = new List<object?> { true, null, false }.ToAsyncEnumerable();

        // Act
        var resultPipe = (IDataPipe)method.Invoke(null, [input, "test-stream"])!;
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
    public async Task AdaptJoinOutput_WithBoolNullItems_YieldsDefaultFalse()
    {
        // Arrange
        var method = typeof(NodeExecutor).GetMethod("AdaptJoinOutput", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(bool));

        var input = new List<object?> { true, null, false }.ToAsyncEnumerable();
        var inputPipe = new StreamingDataPipe<object?>(input);

        // Act
        var resultPipe = (IDataPipe)method.Invoke(null, [inputPipe, "test-stream"])!;
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
