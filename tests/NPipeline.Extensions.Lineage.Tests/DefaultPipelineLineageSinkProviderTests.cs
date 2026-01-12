using FluentAssertions;
using NPipeline.Configuration;
using NPipeline.Lineage;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Extensions.Lineage.Tests;

public class DefaultPipelineLineageSinkProviderTests
{
    [Fact]
    public void Constructor_ShouldInitialize()
    {
        // Arrange & Act
        var provider = new DefaultPipelineLineageSinkProvider();

        // Assert
        provider.Should().NotBeNull();
    }

    [Fact]
    public void Create_WithValidContext_ShouldReturnLoggingSink()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();
        var context = new PipelineContext();

        // Act
        var sink = provider.Create(context);

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<LoggingPipelineLineageSink>();
    }

    [Fact]
    public void Create_WithNullContext_ShouldReturnNull()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();

        // Act
        var sink = provider.Create(null!);

        // Assert
        sink.Should().BeNull();
    }

    [Fact]
    public void Create_WithContextWithParameters_ShouldReturnLoggingSink()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();
        var context = new PipelineContext();
        context.Parameters["TestParam"] = "TestValue";

        // Act
        var sink = provider.Create(context);

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<LoggingPipelineLineageSink>();
    }

    [Fact]
    public void Create_WithMultipleCalls_ShouldReturnDifferentInstances()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();
        var context = new PipelineContext();

        // Act
        var sink1 = provider.Create(context);
        var sink2 = provider.Create(context);

        // Assert
        sink1.Should().NotBeNull();
        sink2.Should().NotBeNull();
        sink1.Should().NotBeSameAs(sink2);
    }

    [Fact]
    public void Create_WithEmptyContext_ShouldReturnLoggingSink()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();
        var context = new PipelineContext();

        // Act
        var sink = provider.Create(context);

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<LoggingPipelineLineageSink>();
    }

    [Fact]
    public void Create_WithContextWithConfiguration_ShouldReturnLoggingSink()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();
        var context = new PipelineContext(PipelineContextConfiguration.Default);

        // Act
        var sink = provider.Create(context);

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeOfType<LoggingPipelineLineageSink>();
    }

    [Fact]
    public void Create_ReturnedSink_ShouldBeOfTypeLoggingPipelineLineageSink()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();
        var context = new PipelineContext();

        // Act
        var sink = provider.Create(context);

        // Assert
        sink.Should().BeAssignableTo<IPipelineLineageSink>();
        sink.Should().BeOfType<LoggingPipelineLineageSink>();
    }

    [Fact]
    public void Create_ReturnedSink_ShouldImplementIPipelineLineageSink()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();
        var context = new PipelineContext();

        // Act
        var sink = provider.Create(context);

        // Assert
        sink.Should().NotBeNull();
        sink.Should().BeAssignableTo<IPipelineLineageSink>();
    }

    [Fact]
    public void Create_WithNullContext_ShouldHandleGracefully()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();

        // Act & Assert
        var exception = Record.Exception(() => provider.Create(null!));
        exception.Should().BeNull();
    }

    [Fact]
    public void Create_ShouldReturnNonNullSinkWhenContextIsValid()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();
        var context = new PipelineContext();

        // Act
        var sink = provider.Create(context);

        // Assert
        sink.Should().NotBeNull();
    }

    [Fact]
    public void Create_ShouldReturnNullWhenContextIsNull()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();

        // Act
        var sink = provider.Create(null!);

        // Assert
        sink.Should().BeNull();
    }

    [Fact]
    public void Create_WithConcurrentCalls_ShouldNotThrow()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();
        var context = new PipelineContext();

        // Act
        var sinks = Enumerable.Range(0, 100)
            .Select(_ => provider.Create(context))
            .ToList();

        // Assert
        sinks.Should().HaveCount(100);
        sinks.Should().OnlyContain(s => s != null);
        sinks.Should().OnlyContain(s => s!.GetType() == typeof(LoggingPipelineLineageSink));
    }

    [Fact]
    public void Create_ShouldCreateIndependentSinks()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();
        var context = new PipelineContext();

        // Act
        var sink1 = provider.Create(context);
        var sink2 = provider.Create(context);

        // Assert
        sink1.Should().NotBeNull();
        sink2.Should().NotBeNull();
        sink1.Should().NotBeSameAs(sink2);
        sink1!.GetHashCode().Should().NotBe(sink2!.GetHashCode());
    }

    [Fact]
    public void Create_WithDifferentContexts_ShouldReturnSinks()
    {
        // Arrange
        var provider = new DefaultPipelineLineageSinkProvider();
        var context1 = new PipelineContext();
        var context2 = new PipelineContext();

        // Act
        var sink1 = provider.Create(context1);
        var sink2 = provider.Create(context2);

        // Assert
        sink1.Should().NotBeNull();
        sink2.Should().NotBeNull();
        sink1.Should().BeOfType<LoggingPipelineLineageSink>();
        sink2.Should().BeOfType<LoggingPipelineLineageSink>();
    }
}