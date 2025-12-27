using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Execution.Pooling;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Pipeline;

public sealed class PipelineContextPoolingTests
{
    [Fact]
    public async Task DisposeAsync_WithOwnedDictionaries_ClearsUserAddedEntries()
    {
        // Arrange
        var context = new PipelineContext();
        var parameters = context.Parameters;
        var items = context.Items;
        var properties = context.Properties;

        parameters["foo"] = "bar";
        items["answer"] = 42;
        properties["flag"] = true;

        // Act
        await context.DisposeAsync();

        // Assert - user-added entries are cleared before pooling
        // (Framework-managed entries like retry options may still be present)
        _ = parameters.Should().NotContainKey("foo");
        _ = items.Should().NotContainKey("answer");
        _ = properties.Should().NotContainKey("flag");
    }

    [Fact]
    public async Task DisposeAsync_WithProvidedDictionaries_DoesNotReturnCallerInstances()
    {
        // Arrange
        var parameters = new Dictionary<string, object> { ["foo"] = "bar" };
        var items = new Dictionary<string, object> { ["answer"] = 42 };
        var properties = new Dictionary<string, object> { ["flag"] = true };

        var config = new PipelineContextConfiguration(
            parameters,
            items,
            properties);

        var context = new PipelineContext(config);

        // Act
        await context.DisposeAsync();

        // Assert - dictionaries remain untouched because context does not own them
        _ = parameters.Should().ContainKey("foo");
        _ = items.Should().ContainKey("answer");
        _ = properties.Should().ContainKey("flag");

        // Pooled dictionary rent should not hand back caller-owned dictionaries
        var rented = PipelineObjectPool.RentStringObjectDictionary();
        _ = rented.Should().NotBeSameAs(parameters);
        _ = rented.Should().NotBeSameAs(items);
        _ = rented.Should().NotBeSameAs(properties);

        PipelineObjectPool.Return(rented);
    }
}
