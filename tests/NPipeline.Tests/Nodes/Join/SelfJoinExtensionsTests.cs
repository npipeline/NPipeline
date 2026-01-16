using System.Collections.Concurrent;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Join;

/// <summary>
///     Comprehensive tests for the AddSelfJoin extension method functionality.
///     Tests cover various join types, edge cases, and error scenarios to ensure
///     the self-join feature works correctly for joining same-type items from different sources.
/// </summary>
public sealed class SelfJoinExtensionsTests
{
    #region Null Fallbacks Tests

    [Fact]
    public async Task AddSelfJoin_NullFallbacks_UsesDefaultBehavior()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<NullFallbacksPipeline>(context);

        // Assert - Should use default projection behavior
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(2, "Should include all left items with default projection");
    }

    #endregion

    #region Test Sink Node

    private sealed class OutputRecordSink(ConcurrentQueue<OutputRecord> store) : SinkNode<OutputRecord>
    {
        public override async Task ExecuteAsync(IDataPipe<OutputRecord> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }
    }

    #endregion

    #region Basic Inner Join Tests

    [Fact]
    public async Task AddSelfJoin_BasicInnerJoin_CorrectlyJoinsMatchingItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<BasicInnerJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(2, "Only matching items should be produced in inner join");

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(1, "Left-A", "Right-A", "Left-A-Right-A"),
            "Should join items with Id=1"
        );

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(2, "Left-B", "Right-B", "Left-B-Right-B"),
            "Should join items with Id=2"
        );
    }

    [Fact]
    public async Task AddSelfJoin_WithSameKeySelector_CorrectlyJoinsItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<SameKeySelectorPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(2);
    }

    #endregion

    #region Left Outer Join Tests

    [Fact]
    public async Task AddSelfJoin_LeftOuterJoin_IncludesUnmatchedLeftItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<LeftOuterJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(3, "All left items should be included");

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(1, "Left-A", "Right-A", "matched"),
            "Should join matching items"
        );

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(2, "Left-B", "Right-B", "matched"),
            "Should join matching items"
        );

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(3, "Left-C", "NO_MATCH", "left_only"),
            "Should include unmatched left item with fallback"
        );
    }

    [Fact]
    public async Task AddSelfJoin_LeftOuterJoin_ExcludesUnmatchedRightItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<LeftOuterJoinExcludesRightPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(1, "Only left items should be included");

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(1, "Left-A", "Right-A", "matched"),
            "Should join matching items"
        );

        resultStore.Should().NotContain(item => item.RightValue == "Right-B",
            "Unmatched right items should not be included");
    }

    #endregion

    #region Right Outer Join Tests

    [Fact]
    public async Task AddSelfJoin_RightOuterJoin_IncludesUnmatchedRightItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<RightOuterJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(3, "All right items should be included");

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(1, "Left-A", "Right-A", "matched"),
            "Should join matching items"
        );

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(2, "Left-B", "Right-B", "matched"),
            "Should join matching items"
        );

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(3, "NO_MATCH", "Right-C", "right_only"),
            "Should include unmatched right item with fallback"
        );
    }

    [Fact]
    public async Task AddSelfJoin_RightOuterJoin_ExcludesUnmatchedLeftItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<RightOuterJoinExcludesLeftPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(1, "Only right items should be included");

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(1, "Left-A", "Right-A", "matched"),
            "Should join matching items"
        );

        resultStore.Should().NotContain(item => item.LeftValue == "Left-B",
            "Unmatched left items should not be included");
    }

    #endregion

    #region Full Outer Join Tests

    [Fact]
    public async Task AddSelfJoin_FullOuterJoin_IncludesAllItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<FullOuterJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(4, "All items from both streams should be included");

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(1, "Left-A", "Right-A", "matched"),
            "Should join matching items"
        );

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(2, "Left-B", "Right-B", "matched"),
            "Should join matching items"
        );

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(3, "Left-C", "NO_MATCH", "left_only"),
            "Should include unmatched left item with fallback"
        );

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(4, "NO_MATCH", "Right-D", "right_only"),
            "Should include unmatched right item with fallback"
        );
    }

    [Fact]
    public async Task AddSelfJoin_FullOuterJoin_UsesBothFallbacks()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<FullOuterJoinBothFallbacksPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(2);

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(1, "Left-A", "NO_MATCH", "left_fallback"),
            "Should use left fallback for unmatched left item"
        );

        resultStore.Should().ContainEquivalentOf(
            new OutputRecord(2, "NO_MATCH", "Right-B", "right_fallback"),
            "Should use right fallback for unmatched right item"
        );
    }

    #endregion

    #region Different Key Selectors Tests

    [Fact]
    public async Task AddSelfJoin_DifferentKeySelectors_CorrectlyMatchesItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<DifferentKeySelectorsPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(2, "Should match items with same key from different selectors");
    }

    [Fact]
    public async Task AddSelfJoin_DifferentKeySelectors_WithNullRightSelector_UsesLeftSelector()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<NullRightSelectorPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(2, "Should use left key selector for both streams when right is null");
    }

    #endregion

    #region No Matches Tests

    [Fact]
    public async Task AddSelfJoin_NoMatches_InnerJoin_ReturnsEmpty()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<NoMatchesInnerJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().BeEmpty("Inner join with no matches should return empty");
    }

    [Fact]
    public async Task AddSelfJoin_NoMatches_LeftOuterJoin_ReturnsAllLeftItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<NoMatchesLeftOuterJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(2, "Left outer join with no matches should return all left items");
        resultStore.Should().AllSatisfy(item => item.RightValue.Should().Be("NO_MATCH"));
    }

    [Fact]
    public async Task AddSelfJoin_NoMatches_RightOuterJoin_ReturnsAllRightItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<NoMatchesRightOuterJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(2, "Right outer join with no matches should return all right items");
        resultStore.Should().AllSatisfy(item => item.LeftValue.Should().Be("NO_MATCH"));
    }

    [Fact]
    public async Task AddSelfJoin_NoMatches_FullOuterJoin_ReturnsAllItems()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<NoMatchesFullOuterJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(4, "Full outer join with no matches should return all items from both streams");
    }

    #endregion

    #region Multiple Matches Tests

    [Fact]
    public async Task AddSelfJoin_MultipleMatches_ProducesAllCombinations()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<MultipleMatchesPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();

        // 2 left items with Id=1 × 3 right items with Id=1 = 6 combinations
        // 1 left item with Id=2 × 0 right items with Id=2 = 0 combinations
        resultStore.Should().HaveCount(6, "Should produce all combinations of matching items");

        // Verify all combinations are present
        var combinations = resultStore
            .Where(item => item.Id == 1)
            .Select(item => (item.LeftValue, item.RightValue))
            .ToList();

        combinations.Should().HaveCount(6);
    }

    [Fact]
    public async Task AddSelfJoin_MultipleMatches_WithOuterJoin_IncludesAllCombinationsAndUnmatched()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<MultipleMatchesOuterJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();

        // 2 left items with Id=1 × 2 right items with Id=1 = 4 combinations
        // 1 left item with Id=2 (unmatched) = 1 item
        resultStore.Should().HaveCount(5, "Should include all combinations and unmatched items");
    }

    #endregion

    #region Error Handling Tests

    [Fact]
    public void AddSelfJoin_NullBuilder_ThrowsArgumentNullException()
    {
        // Arrange
        PipelineBuilder? nullBuilder = null;
        var leftSource = new SourceNodeHandle<TestRecord>("left_source");
        var rightSource = new SourceNodeHandle<TestRecord>("right_source");

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
        {
            nullBuilder!.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id);
        });
    }

    [Fact]
    public void AddSelfJoin_NullLeftSource_ThrowsArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        SourceNodeHandle<TestRecord>? nullLeftSource = null;
        var rightSource = new SourceNodeHandle<TestRecord>("right_source");

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
        {
            builder.AddSelfJoin(
                nullLeftSource!,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id);
        });
    }

    [Fact]
    public void AddSelfJoin_NullRightSource_ThrowsArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var leftSource = new SourceNodeHandle<TestRecord>("left_source");
        SourceNodeHandle<TestRecord>? nullRightSource = null;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
        {
            builder.AddSelfJoin(
                leftSource,
                nullRightSource!,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id);
        });
    }

    [Fact]
    public void AddSelfJoin_NullNodeName_ThrowsArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var leftSource = new SourceNodeHandle<TestRecord>("left_source");
        var rightSource = new SourceNodeHandle<TestRecord>("right_source");

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
        {
            builder.AddSelfJoin(
                leftSource,
                rightSource,
                null!,
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id);
        });
    }

    [Fact]
    public void AddSelfJoin_NullOutputFactory_ThrowsArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var leftSource = new SourceNodeHandle<TestRecord>("left_source");
        var rightSource = new SourceNodeHandle<TestRecord>("right_source");

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
        {
            builder.AddSelfJoin<TestRecord, int, OutputRecord>(
                leftSource,
                rightSource,
                "test_join",
                null!,
                item => item.Id);
        });
    }

    [Fact]
    public void AddSelfJoin_NullLeftKeySelector_ThrowsArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var leftSource = new SourceNodeHandle<TestRecord>("left_source");
        var rightSource = new SourceNodeHandle<TestRecord>("right_source");

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
        {
            builder.AddSelfJoin<TestRecord, int, OutputRecord>(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                null!);
        });
    }

    [Fact]
    public async Task AddSelfJoin_InvalidNodeName_AllowsCustomNames()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<OutputRecord>>();
        services.AddNPipeline(typeof(SelfJoinExtensionsTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<CustomNodeNamePipeline>(context);

        // Assert - Should allow custom node names
        var resultStore = provider.GetRequiredService<ConcurrentQueue<OutputRecord>>();
        resultStore.Should().HaveCount(1);
    }

    #endregion

    #region Test Models

    /// <summary>
    ///     Simple test record with Id and Value properties.
    /// </summary>
    /// <param name="Id">The unique identifier.</param>
    /// <param name="Value">The value associated with the record.</param>
    private sealed record TestRecord(int Id, string Value);

    /// <summary>
    ///     Output record for join results containing combined data from both streams.
    /// </summary>
    /// <param name="Id">The join key identifier.</param>
    /// <param name="LeftValue">The value from the left stream.</param>
    /// <param name="RightValue">The value from the right stream.</param>
    /// <param name="Description">A description of the join result.</param>
    private sealed record OutputRecord(int Id, string LeftValue, string RightValue, string Description);

    #endregion

    #region Pipeline Definitions

    private sealed class BasicInnerJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[]
            {
                new TestRecord(1, "Left-A"),
                new TestRecord(2, "Left-B"),
                new TestRecord(3, "Left-C"),
            };

            var rightData = new[]
            {
                new TestRecord(1, "Right-A"),
                new TestRecord(2, "Right-B"),
                new TestRecord(4, "Right-D"),
            };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(
                    left.Id,
                    left.Value,
                    right.Value,
                    $"{left.Value}-{right.Value}"
                ),
                item => item.Id,
                item => item.Id);

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class SameKeySelectorPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[] { new TestRecord(1, "Left-1"), new TestRecord(2, "Left-2") };
            var rightData = new[] { new TestRecord(1, "Right-1"), new TestRecord(2, "Right-2") };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "merged"),
                item => item.Id,
                joinType: JoinType.Inner);

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class LeftOuterJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[]
            {
                new TestRecord(1, "Left-A"),
                new TestRecord(2, "Left-B"),
                new TestRecord(3, "Left-C"),
            };

            var rightData = new[]
            {
                new TestRecord(1, "Right-A"),
                new TestRecord(2, "Right-B"),
            };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id,
                JoinType.LeftOuter,
                left => new OutputRecord(left.Id, left.Value, "NO_MATCH", "left_only"));

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class LeftOuterJoinExcludesRightPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[] { new TestRecord(1, "Left-A") };
            var rightData = new[] { new TestRecord(1, "Right-A"), new TestRecord(2, "Right-B") };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id,
                JoinType.LeftOuter,
                left => new OutputRecord(left.Id, left.Value, "NO_MATCH", "left_only"));

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class RightOuterJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[]
            {
                new TestRecord(1, "Left-A"),
                new TestRecord(2, "Left-B"),
            };

            var rightData = new[]
            {
                new TestRecord(1, "Right-A"),
                new TestRecord(2, "Right-B"),
                new TestRecord(3, "Right-C"),
            };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id,
                JoinType.RightOuter,
                rightFallback: right => new OutputRecord(right.Id, "NO_MATCH", right.Value, "right_only"));

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class RightOuterJoinExcludesLeftPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[] { new TestRecord(1, "Left-A"), new TestRecord(2, "Left-B") };
            var rightData = new[] { new TestRecord(1, "Right-A") };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id,
                JoinType.RightOuter,
                rightFallback: right => new OutputRecord(right.Id, "NO_MATCH", right.Value, "right_only"));

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class FullOuterJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[]
            {
                new TestRecord(1, "Left-A"),
                new TestRecord(2, "Left-B"),
                new TestRecord(3, "Left-C"),
            };

            var rightData = new[]
            {
                new TestRecord(1, "Right-A"),
                new TestRecord(2, "Right-B"),
                new TestRecord(4, "Right-D"),
            };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id,
                JoinType.FullOuter,
                left => new OutputRecord(left.Id, left.Value, "NO_MATCH", "left_only"),
                right => new OutputRecord(right.Id, "NO_MATCH", right.Value, "right_only"));

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class FullOuterJoinBothFallbacksPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[] { new TestRecord(1, "Left-A") };
            var rightData = new[] { new TestRecord(2, "Right-B") };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id,
                JoinType.FullOuter,
                left => new OutputRecord(left.Id, left.Value, "NO_MATCH", "left_fallback"),
                right => new OutputRecord(right.Id, "NO_MATCH", right.Value, "right_fallback"));

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class DifferentKeySelectorsPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[]
            {
                new TestRecord(1, "A"),
                new TestRecord(2, "B"),
                new TestRecord(3, "C"),
            };

            var rightData = new[]
            {
                new TestRecord(10, "A"),
                new TestRecord(20, "BB"),
                new TestRecord(30, "DDDD"),
            };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Value.Length);

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class NullRightSelectorPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[] { new TestRecord(1, "A"), new TestRecord(2, "B") };
            var rightData = new[] { new TestRecord(1, "C"), new TestRecord(2, "D") };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                joinType: JoinType.Inner);

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class NoMatchesInnerJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[] { new TestRecord(1, "A"), new TestRecord(2, "B") };
            var rightData = new[] { new TestRecord(3, "C"), new TestRecord(4, "D") };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id);

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class NoMatchesLeftOuterJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[] { new TestRecord(1, "A"), new TestRecord(2, "B") };
            var rightData = new[] { new TestRecord(3, "C"), new TestRecord(4, "D") };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id,
                JoinType.LeftOuter,
                left => new OutputRecord(left.Id, left.Value, "NO_MATCH", "left_only"));

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class NoMatchesRightOuterJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[] { new TestRecord(1, "A"), new TestRecord(2, "B") };
            var rightData = new[] { new TestRecord(3, "C"), new TestRecord(4, "D") };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id,
                JoinType.RightOuter,
                rightFallback: right => new OutputRecord(right.Id, "NO_MATCH", right.Value, "right_only"));

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class NoMatchesFullOuterJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[] { new TestRecord(1, "A"), new TestRecord(2, "B") };
            var rightData = new[] { new TestRecord(3, "C"), new TestRecord(4, "D") };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id,
                JoinType.FullOuter,
                left => new OutputRecord(left.Id, left.Value, "NO_MATCH", "left_only"),
                right => new OutputRecord(right.Id, "NO_MATCH", right.Value, "right_only"));

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class MultipleMatchesPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[]
            {
                new TestRecord(1, "Left-A1"),
                new TestRecord(1, "Left-A2"),
                new TestRecord(2, "Left-B"),
            };

            var rightData = new[]
            {
                new TestRecord(1, "Right-A1"),
                new TestRecord(1, "Right-A2"),
                new TestRecord(1, "Right-A3"),
            };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id);

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class MultipleMatchesOuterJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[]
            {
                new TestRecord(1, "Left-A1"),
                new TestRecord(1, "Left-A2"),
                new TestRecord(2, "Left-B"),
            };

            var rightData = new[]
            {
                new TestRecord(1, "Right-A1"),
                new TestRecord(1, "Right-A2"),
            };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id,
                JoinType.FullOuter,
                left => new OutputRecord(left.Id, left.Value, "NO_MATCH", "left_only"),
                right => new OutputRecord(right.Id, "NO_MATCH", right.Value, "right_only"));

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class NullFallbacksPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[] { new TestRecord(1, "Left-A"), new TestRecord(2, "Left-B") };
            var rightData = new[] { new TestRecord(1, "Right-A") };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "test_join",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id,
                item => item.Id,
                JoinType.LeftOuter);

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    private sealed class CustomNodeNamePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var leftData = new[] { new TestRecord(1, "A") };
            var rightData = new[] { new TestRecord(1, "B") };

            var leftSource = builder.AddInMemorySource("left_source", leftData);
            var rightSource = builder.AddInMemorySource("right_source", rightData);

            var joinNode = builder.AddSelfJoin(
                leftSource,
                rightSource,
                "my_custom_join_node_123",
                (left, right) => new OutputRecord(left.Id, left.Value, right.Value, "matched"),
                item => item.Id);

            var sink = builder.AddSink<OutputRecordSink, OutputRecord>("sink");
            builder.Connect(joinNode, sink);
        }
    }

    #endregion
}
