using AwesomeAssertions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests;

/// <summary>
///     Tests for <see cref="EnrichmentNode{T}" /> covering lookup, computation, and default value operations.
///     Scenarios tested:
///     - Lookup: Adding properties from lookup dictionaries with existing and missing keys
///     - Set: Unconditional property setting from lookups
///     - Compute: Setting properties based on computed values
///     - Defaults: Setting defaults for null, empty, whitespace, zero, and conditional cases
///     - Chaining: Multiple enrichment operations in sequence
///     - Cancellation: Pipeline cancellation support
/// </summary>
public sealed class EnrichmentNodeTests
{
    private sealed class TestData
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public int Age { get; set; }
        public string? Description { get; set; }
        public string? Label { get; set; }
        public IEnumerable<string> Tags { get; set; } = [];
    }

    #region Lookup Tests

    [Fact]
    public async Task Lookup_WithExistingKey_ShouldEnrichProperty()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();

        var lookup = new Dictionary<int, string?>
        {
            { 1, "One" },
            { 2, "Two" },
            { 3, "Three" },
        };

        node.Lookup(x => x.Description, lookup, x => x.Id);

        var data = new TestData { Id = 2 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Description.Should().Be("Two");
    }

    [Fact]
    public async Task Lookup_WithMissingKey_ShouldNotModifyProperty()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();

        var lookup = new Dictionary<int, string?>
        {
            { 1, "One" },
            { 2, "Two" },
        };

        node.Lookup(x => x.Description, lookup, x => x.Id);

        var data = new TestData { Id = 99, Description = "Unchanged" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Description.Should().Be("Unchanged");
    }

    [Fact]
    public async Task Set_WithExistingKey_ShouldSetProperty()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();

        var lookup = new Dictionary<int, string?>
        {
            { 1, "One" },
            { 2, "Two" },
        };

        node.Set(x => x.Description, lookup, x => x.Id);

        var data = new TestData { Id = 1, Description = "Original" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Description.Should().Be("One");
    }

    [Fact]
    public async Task Set_WithMissingKey_ShouldSetToDefault()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();

        var lookup = new Dictionary<int, string?>
        {
            { 1, "One" },
        };

        node.Set(x => x.Description, lookup, x => x.Id);

        var data = new TestData { Id = 99, Description = "Original" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Description.Should().BeNull();
        result.Id.Should().Be(99);
    }

    #endregion

    #region Compute Tests

    [Fact]
    public async Task Compute_WithSimpleComputation_ShouldSetProperty()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();
        node.Compute(x => x.Description, item => $"ID: {item.Id}");

        var data = new TestData { Id = 42 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Description.Should().Be("ID: 42");
    }

    [Fact]
    public async Task Compute_WithComplexComputation_ShouldComputeCorrectly()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();

        node.Compute(
            x => x.Description,
            item => $"{item.Name ?? "Unknown"} ({item.Id})");

        var data = new TestData { Id = 1, Name = "John" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Description.Should().Be("John (1)");
    }

    #endregion

    #region Default Value Tests

    [Fact]
    public async Task DefaultIfNull_WithNullValue_ShouldSetDefault()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();
        node.DefaultIfNull(x => x.Name, "DefaultName");

        var data = new TestData { Name = null };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("DefaultName");
    }

    [Fact]
    public async Task DefaultIfNull_WithNonNullValue_ShouldNotChange()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();
        node.DefaultIfNull(x => x.Name, "DefaultName");

        var data = new TestData { Name = "John" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("John");
    }

    [Fact]
    public async Task DefaultIfEmpty_WithEmptyString_ShouldSetDefault()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();
        node.DefaultIfEmpty(x => x.Name, "DefaultName");

        var data = new TestData { Name = "" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("DefaultName");
    }

    [Fact]
    public async Task DefaultIfWhitespace_WithWhitespaceString_ShouldSetDefault()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();
        node.DefaultIfWhitespace(x => x.Name, "DefaultName");

        var data = new TestData { Name = "   " };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("DefaultName");
    }

    [Fact]
    public async Task DefaultWhen_WithConditionTrue_ShouldSetDefault()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();
        node.DefaultWhen(x => x.Age, age => age < 18, 18);

        var data = new TestData { Age = 10 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Age.Should().Be(18);
    }

    [Fact]
    public async Task DefaultIfZero_WithZeroInteger_ShouldSetDefault()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();
        node.DefaultIfZero(x => x.Age, 18);

        var data = new TestData { Age = 0 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Age.Should().Be(18);
    }

    [Fact]
    public async Task DefaultIfEmptyCollection_WithEmptyCollection_ShouldSetDefault()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();
        var defaultTags = new[] { "default" };
        node.DefaultIfEmptyCollection(x => x.Tags, defaultTags);

        var data = new TestData { Tags = Array.Empty<string>() };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Tags.Should().Equal("default");
    }

    [Fact]
    public async Task DefaultIfEmptyCollection_WithNullCollection_ShouldSetDefault()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();
        var defaultTags = new[] { "default" };
        node.DefaultIfEmptyCollection(x => x.Tags, defaultTags);

        var data = new TestData { Tags = null! };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Tags.Should().Equal("default");
    }

    #endregion

    #region Combined Operations Tests

    [Fact]
    public async Task ChainedOperations_WithLookupAndDefaults_ShouldApplyAll()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();
        var statusLookup = new Dictionary<int, string?> { { 1, "Active" } };

        node.Lookup(x => x.Description, statusLookup, x => x.Id)
            .DefaultIfNull(x => x.Name, "UnknownName")
            .DefaultIfZero(x => x.Age, 18)
            .Compute(x => x.Label, item => $"{item.Name} - {item.Description}");

        var data = new TestData { Id = 1, Name = null, Age = 0 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Description.Should().Be("Active");
        result.Name.Should().Be("UnknownName");
        result.Age.Should().Be(18);
        result.Label.Should().Be("UnknownName - Active");
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellation_ShouldThrowOperationCanceledException()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();
        var lookup = new Dictionary<int, string?> { { 1, "One" } };
        node.Lookup(x => x.Description, lookup, x => x.Id);

        var data = new TestData { Id = 1 };
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await node.ExecuteAsync(data, context, cts.Token));
    }

    [Fact]
    public async Task ExecuteAsync_WithNoOperations_ShouldReturnUnchanged()
    {
        // Arrange
        var node = new EnrichmentNode<TestData>();
        var data = new TestData { Id = 1, Name = "Test" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Should().BeSameAs(data);
        result.Id.Should().Be(1);
        result.Name.Should().Be("Test");
    }

    #endregion
}
