using NPipeline.Extensions.Nodes.Core;
using NPipeline.Extensions.Nodes.Core.Exceptions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests.Core;

public class CollectionValidationNodeTests
{
    #region HasMinCount Tests

    [Fact]
    public async Task HasMinCount_WithInsufficientItems_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.HasMinCount(x => x.Items, 5);

        var item = new TestObject { Items = ["a", "b", "c"] };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    private sealed class TestObject
    {
        public IEnumerable<string>? Items { get; set; }
        public IEnumerable<int>? Numbers { get; set; }
    }

    #region HasMaxCount Tests

    [Fact]
    public async Task HasMaxCount_WithinLimit_Passes()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.HasMaxCount(x => x.Items, 5);

        var item = new TestObject { Items = ["a", "b", "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task HasMaxCount_ExceedsLimit_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.HasMaxCount(x => x.Items, 2);

        var item = new TestObject { Items = ["a", "b", "c"] };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region HasCountBetween Tests

    [Fact]
    public async Task HasCountBetween_WithinRange_Passes()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.HasCountBetween(x => x.Items, 2, 5);

        var item = new TestObject { Items = ["a", "b", "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task HasCountBetween_TooFew_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.HasCountBetween(x => x.Items, 5, 10);

        var item = new TestObject { Items = ["a", "b", "c"] };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task HasCountBetween_TooMany_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.HasCountBetween(x => x.Items, 1, 2);

        var item = new TestObject { Items = ["a", "b", "c"] };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsNotEmpty Tests

    [Fact]
    public async Task IsNotEmpty_WithItems_Passes()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.IsNotEmpty(x => x.Items);

        var item = new TestObject { Items = ["a", "b", "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsNotEmpty_WithEmptyCollection_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.IsNotEmpty(x => x.Items);

        var item = new TestObject { Items = [] };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task IsNotEmpty_WithNull_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.IsNotEmpty(x => x.Items);

        var item = new TestObject { Items = null };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region AllMatch Tests

    [Fact]
    public async Task AllMatch_WithAllMatching_Passes()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.AllMatch(x => x.Numbers, n => n > 0);

        var item = new TestObject { Numbers = [1, 2, 3] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task AllMatch_WithSomeNotMatching_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.AllMatch(x => x.Numbers, n => n > 2);

        var item = new TestObject { Numbers = [1, 2, 3] };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region AnyMatch Tests

    [Fact]
    public async Task AnyMatch_WithAtLeastOneMatching_Passes()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.AnyMatch(x => x.Numbers, n => n > 2);

        var item = new TestObject { Numbers = [1, 2, 3] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task AnyMatch_WithNoneMatching_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.AnyMatch(x => x.Numbers, n => n > 10);

        var item = new TestObject { Numbers = [1, 2, 3] };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region NoneMatch Tests

    [Fact]
    public async Task NoneMatch_WithNoneMatching_Passes()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.NoneMatch(x => x.Numbers, n => n > 10);

        var item = new TestObject { Numbers = [1, 2, 3] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task NoneMatch_WithAtLeastOneMatching_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.NoneMatch(x => x.Numbers, n => n > 2);

        var item = new TestObject { Numbers = [1, 2, 3] };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region Contains Tests

    [Fact]
    public async Task Contains_WithContainedItem_Passes()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.Contains(x => x.Items, "b");

        var item = new TestObject { Items = ["a", "b", "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task Contains_WithoutContainedItem_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.Contains(x => x.Items, "z");

        var item = new TestObject { Items = ["a", "b", "c"] };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task Contains_WithNull_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.Contains(x => x.Items, "a");

        var item = new TestObject { Items = null };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region DoesNotContain Tests

    [Fact]
    public async Task DoesNotContain_WithoutItem_Passes()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.DoesNotContain(x => x.Items, "z");

        var item = new TestObject { Items = ["a", "b", "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task DoesNotContain_WithContainedItem_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.DoesNotContain(x => x.Items, "b");

        var item = new TestObject { Items = ["a", "b", "c"] };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task DoesNotContain_WithNull_Passes()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.DoesNotContain(x => x.Items, "a");

        var item = new TestObject { Items = null };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    #endregion

    #region AllUnique Tests

    [Fact]
    public async Task AllUnique_WithUniqueItems_Passes()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.AllUnique(x => x.Items);

        var item = new TestObject { Items = ["a", "b", "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task AllUnique_WithDuplicates_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.AllUnique(x => x.Items);

        var item = new TestObject { Items = ["a", "b", "a"] };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    #endregion

    #region IsSubsetOf Tests

    [Fact]
    public async Task IsSubsetOf_WithValidSubset_Passes()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.IsSubsetOf(x => x.Items, ["a", "b", "c", "d", "e"]);

        var item = new TestObject { Items = ["a", "b", "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task IsSubsetOf_WithItemsNotInSet_Throws()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.IsSubsetOf(x => x.Items, ["a", "b", "c"]);

        var item = new TestObject { Items = ["a", "b", "z"] };
        await Assert.ThrowsAsync<ValidationException>(() => node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None));
    }

    [Fact]
    public async Task IsSubsetOf_WithNull_Passes()
    {
        var node = new CollectionValidationNode<TestObject>();
        node.IsSubsetOf(x => x.Items, ["a", "b", "c"]);

        var item = new TestObject { Items = null };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result);
    }

    #endregion
}
