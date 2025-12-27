using AwesomeAssertions;
using NPipeline.Lineage;

namespace NPipeline.Tests.Lineage;

/// <summary>
///     Tests for <see cref="DefaultLineageFactory" /> covering lineage sink creation and resolution.
/// </summary>
public sealed class DefaultLineageFactoryTests
{
    private readonly DefaultLineageFactory _factory = new();

    #region CreateLineageSink Tests

    [Fact]
    public void CreateLineageSink_WithValidLineageSinkType_ReturnsInstance()
    {
        // Arrange
        var sinkType = typeof(TestLineageSink);

        // Act
        var result = _factory.CreateLineageSink(sinkType);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<TestLineageSink>();
    }

    [Fact]
    public void CreateLineageSink_WithNonImplementingType_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(double);

        // Act
        var result = _factory.CreateLineageSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreateLineageSink_WithTypeWithoutParameterlessConstructor_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(TypeWithoutParameterlessConstructor);

        // Act
        var result = _factory.CreateLineageSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreateLineageSink_WithThrowingConstructor_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(ThrowingLineageSinkConstructor);

        // Act
        var result = _factory.CreateLineageSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    #endregion

    #region CreatePipelineLineageSink Tests

    [Fact]
    public void CreatePipelineLineageSink_WithValidPipelineLineageSinkType_ReturnsInstance()
    {
        // Arrange
        var sinkType = typeof(TestPipelineLineageSink);

        // Act
        var result = _factory.CreatePipelineLineageSink(sinkType);

        // Assert
        result.Should().NotBeNull();
        result.Should().BeOfType<TestPipelineLineageSink>();
    }

    [Fact]
    public void CreatePipelineLineageSink_WithNonImplementingType_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(DateTime);

        // Act
        var result = _factory.CreatePipelineLineageSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreatePipelineLineageSink_WithTypeWithoutParameterlessConstructor_ReturnsNull()
    {
        // Arrange
        var sinkType = typeof(TypeWithoutParameterlessConstructor);

        // Act
        var result = _factory.CreatePipelineLineageSink(sinkType);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void CreatePipelineLineageSink_MultipleInvocations_CreatesNewInstanceEachTime()
    {
        // Arrange
        var sinkType = typeof(TestPipelineLineageSink);

        // Act
        var result1 = _factory.CreatePipelineLineageSink(sinkType);
        var result2 = _factory.CreatePipelineLineageSink(sinkType);

        // Assert
        result1.Should().NotBeNull();
        result2.Should().NotBeNull();
        result1.Should().NotBeSameAs(result2);
    }

    #endregion

    #region Resolution Tests

    [Fact]
    public void ResolvePipelineLineageSinkProvider_WithoutDIContainer_ReturnsNull()
    {
        // Act
        var result = _factory.ResolvePipelineLineageSinkProvider();

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void ResolveLineageCollector_WithoutDIContainer_ReturnsNull()
    {
        // Act
        var result = _factory.ResolveLineageCollector();

        // Assert
        result.Should().BeNull();
    }

    #endregion

    #region Test Fixtures

    private sealed class TestLineageSink : ILineageSink
    {
        public Task RecordAsync(
            LineageInfo lineageInfo,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TestPipelineLineageSink : IPipelineLineageSink
    {
        public Task RecordAsync(
            PipelineLineageReport report,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingLineageSinkConstructor : ILineageSink
    {
        public ThrowingLineageSinkConstructor()
        {
            throw new IOException("Disk failure during init");
        }

        public Task RecordAsync(
            LineageInfo lineageInfo,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    private sealed class TypeWithoutParameterlessConstructor
    {
        public TypeWithoutParameterlessConstructor(string _)
        {
        }
    }

    #endregion
}
