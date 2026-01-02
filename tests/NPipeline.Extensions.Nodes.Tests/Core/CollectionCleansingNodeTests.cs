#nullable disable warnings
using NPipeline.Extensions.Nodes.Core;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests.Core;

public class CollectionCleansingNodeTests
{
    private sealed class TestObject
    {
        public IEnumerable<string?>? Items { get; set; }
        public IEnumerable<string>? NonNullableItems { get; set; }
        public IEnumerable<int>? Numbers { get; set; }
    }

    #region RemoveNulls Tests

    [Fact]
    public async Task RemoveNulls_RemovesNullItems()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.RemoveNulls(x => x.Items);

        var item = new TestObject { Items = ["a", null, "b", null, "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Items);
        Assert.Equal(3, result.Items.Where(x => x != null).Count());
    }

    [Fact]
    public async Task RemoveNulls_WithNoNulls_NoChange()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.RemoveNulls(x => x.Items);

        var item = new TestObject { Items = ["a", "b", "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Items);
        Assert.Equal(3, result.Items.Count());
    }

    #endregion

    #region RemoveDuplicates Tests

    [Fact]
    public async Task RemoveDuplicates_RemovesDuplicateItems()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.RemoveDuplicates(x => x.Items);

        var item = new TestObject { Items = ["a", "b", "a", "c", "b"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Items);
        var resultList = result.Items.OfType<string>().ToList();
        Assert.Equal(3, resultList.Count);
        Assert.Contains("a", resultList);
        Assert.Contains("b", resultList);
        Assert.Contains("c", resultList);
    }

    [Fact]
    public async Task RemoveDuplicates_WithNoduplicates_NoChange()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.RemoveDuplicates(x => x.Items);

        var item = new TestObject { Items = ["a", "b", "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Items);
        Assert.Equal(3, result.Items.Count());
    }

    #endregion

    #region RemoveEmpty Tests

    [Fact]
    public async Task RemoveEmpty_RemovesEmptyStrings()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.RemoveEmpty(x => x.Items);

        var item = new TestObject { Items = ["a", "", "b", "", "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Items);
        Assert.Equal(3, result.Items.OfType<string>().Count());
    }

    [Fact]
    public async Task RemoveEmpty_WithNoEmpty_NoChange()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.RemoveEmpty(x => x.Items);

        var item = new TestObject { Items = ["a", "b", "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Items);
        Assert.Equal(3, result.Items.Count());
    }

    #endregion

    #region RemoveWhitespace Tests

    [Fact]
    public async Task RemoveWhitespace_RemovesWhitespaceStrings()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.RemoveWhitespace(x => x.Items);

        var item = new TestObject { Items = ["a", "   ", "b", "\t", "c"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Items);
        Assert.Equal(3, result.Items.Count());
    }

    #endregion

    #region Sort Tests

    [Fact]
    public async Task Sort_SortsNumbers()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.Sort(x => x.Numbers);

        var item = new TestObject { Numbers = [3, 1, 4, 1, 5, 9] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Numbers);
        var resultList = result.Numbers.ToList();
        Assert.True(resultList[0] <= resultList[1] && resultList[1] <= resultList[2]);
    }

    [Fact]
    public async Task Sort_SortsStrings()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.Sort(x => x.NonNullableItems);

        var item = new TestObject { NonNullableItems = ["c", "a", "b"] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.NonNullableItems);
        var resultList = result.NonNullableItems.ToList();
        Assert.True(resultList[0] == "a" && resultList[1] == "b" && resultList[2] == "c");
    }

    #endregion

    #region Reverse Tests

    [Fact]
    public async Task Reverse_RevergesOrder()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.Reverse(x => x.Numbers);

        var item = new TestObject { Numbers = [1, 2, 3, 4, 5] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Numbers);
        var resultList = result.Numbers.ToList();
        Assert.Equal(5, resultList[0]);
        Assert.Equal(1, resultList[4]);
    }

    #endregion

    #region Take Tests

    [Fact]
    public async Task Take_TakesFirstN()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.Take(x => x.Numbers, 3);

        var item = new TestObject { Numbers = [1, 2, 3, 4, 5] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Numbers);
        Assert.Equal(3, result.Numbers.Count());
    }

    [Fact]
    public async Task Take_WithMoreThanAvailable_TakesAll()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.Take(x => x.Numbers, 10);

        var item = new TestObject { Numbers = [1, 2, 3] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Numbers);
        Assert.Equal(3, result.Numbers.Count());
    }

    #endregion

    #region Skip Tests

    [Fact]
    public async Task Skip_SkipsFirstN()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.Skip(x => x.Numbers, 2);

        var item = new TestObject { Numbers = [1, 2, 3, 4, 5] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Numbers);
        Assert.Equal(3, result.Numbers.Count());
        var resultList = result.Numbers.ToList();
        Assert.Equal(3, resultList[0]);
    }

    [Fact]
    public async Task Skip_WithMoreThanAvailable_EmptyResult()
    {
        var node = new CollectionCleansingNode<TestObject>();
        node.Skip(x => x.Numbers, 10);

        var item = new TestObject { Numbers = [1, 2, 3] };
        var result = await node.ExecuteAsync(item, PipelineContext.Default, CancellationToken.None);
        Assert.NotNull(result.Numbers);
        Assert.Empty(result.Numbers);
    }

    #endregion
}
