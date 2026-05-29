using System.Collections.Concurrent;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Pipeline;

public sealed class PipelineContextDictionaryTests
{
    [Fact]
    public async Task DefaultProfile_ItemsShouldBeConcurrentDictionary()
    {
        await using var context = new PipelineContext(new PipelineContextConfiguration
        {
            OptimizationProfile = PipelineOptimizationProfile.Default
        });

        context.Items.Should().BeOfType<ConcurrentDictionary<string, object>>();
    }

    [Fact]
    public async Task DefaultProfile_ParametersShouldBeConcurrentDictionary()
    {
        await using var context = new PipelineContext(new PipelineContextConfiguration
        {
            OptimizationProfile = PipelineOptimizationProfile.Default
        });

        context.Parameters.Should().BeOfType<ConcurrentDictionary<string, object>>();
    }

    [Fact]
    public async Task DefaultProfile_PropertiesShouldBeConcurrentDictionary()
    {
        await using var context = new PipelineContext(new PipelineContextConfiguration
        {
            OptimizationProfile = PipelineOptimizationProfile.Default
        });

        context.Properties.Should().BeOfType<ConcurrentDictionary<string, object>>();
    }

    [Fact]
    public async Task HighThroughputProfile_ItemsShouldBeDictionary()
    {
        await using var context = new PipelineContext(new PipelineContextConfiguration
        {
            OptimizationProfile = PipelineOptimizationProfile.HighThroughput
        });

        context.Items.Should().BeOfType<Dictionary<string, object>>();
    }

    [Fact]
    public async Task HighThroughputProfile_ParametersShouldBeDictionary()
    {
        await using var context = new PipelineContext(new PipelineContextConfiguration
        {
            OptimizationProfile = PipelineOptimizationProfile.HighThroughput
        });

        context.Parameters.Should().BeOfType<Dictionary<string, object>>();
    }

    [Fact]
    public async Task HighThroughputProfile_PropertiesShouldBeDictionary()
    {
        await using var context = new PipelineContext(new PipelineContextConfiguration
        {
            OptimizationProfile = PipelineOptimizationProfile.HighThroughput
        });

        context.Properties.Should().BeOfType<Dictionary<string, object>>();
    }

    [Fact]
    public async Task DefaultProfile_ConcurrentWritesShouldNotThrow()
    {
        await using var context = new PipelineContext(new PipelineContextConfiguration
        {
            OptimizationProfile = PipelineOptimizationProfile.Default
        });

        var exceptions = new List<Exception>();

        var tasks = Enumerable.Range(0, 100)
            .Select(i => Task.Run(() =>
            {
                try
                {
                    context.Items[$"key-{i}"] = i;
                    _ = context.Items[$"key-{i}"];
                }
                catch (Exception ex)
                {
                    lock (exceptions) exceptions.Add(ex);
                }
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        exceptions.Should().BeEmpty();
    }

    [Fact]
    public async Task DisposeAsync_WithDefaultProfileOwnedDictionaries_ShouldClearEntries()
    {
        var context = new PipelineContext(new PipelineContextConfiguration
        {
            OptimizationProfile = PipelineOptimizationProfile.Default
        });

        var parameters = context.Parameters;
        var items = context.Items;
        var properties = context.Properties;

        parameters["p"] = 1;
        items["i"] = 2;
        properties["x"] = 3;

        await context.DisposeAsync();

        parameters.Should().BeEmpty();
        items.Should().BeEmpty();
        properties.Should().BeEmpty();
    }

    [Fact]
    public async Task ExecutionConfiguration_OptimizationProfile_Default()
    {
        await using var context = new PipelineContext(new PipelineContextConfiguration
        {
            OptimizationProfile = PipelineOptimizationProfile.Default
        });

        context.ExecutionConfiguration.OptimizationProfile.Should().Be(PipelineOptimizationProfile.Default);
    }

    [Fact]
    public async Task ExecutionConfiguration_OptimizationProfile_HighThroughput()
    {
        await using var context = new PipelineContext(new PipelineContextConfiguration
        {
            OptimizationProfile = PipelineOptimizationProfile.HighThroughput
        });

        context.ExecutionConfiguration.OptimizationProfile.Should().Be(PipelineOptimizationProfile.HighThroughput);
    }
}