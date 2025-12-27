using AwesomeAssertions;
using NPipeline.Observability;

namespace NPipeline.Tests.Observability;

/// <summary>
///     Tests for <see cref="DefaultObservabilityFactory" /> covering observability collector resolution.
/// </summary>
public sealed class DefaultObservabilityFactoryTests
{
    private readonly DefaultObservabilityFactory _factory = new();

    [Fact]
    public void ResolveObservabilityCollector_WithoutDIContainer_ReturnsNull()
    {
        // Act
        var result = _factory.ResolveObservabilityCollector();

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void ResolveObservabilityCollector_MultipleInvocations_AlwaysReturnsNull()
    {
        // Act
        var result1 = _factory.ResolveObservabilityCollector();
        var result2 = _factory.ResolveObservabilityCollector();

        // Assert
        result1.Should().BeNull();
        result2.Should().BeNull();
    }
}
