using AwesomeAssertions;
using NPipeline.Connectors.Azure.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Configuration;

public class CosmosAuthenticationModeTests
{
    [Fact]
    public void ConnectionString_ShouldHaveCorrectValue()
    {
        // Act
        var mode = CosmosAuthenticationMode.ConnectionString;

        // Assert
        _ = mode.Should().Be(CosmosAuthenticationMode.ConnectionString);
        ((int)mode).Should().Be(0);
    }

    [Fact]
    public void AccountEndpointAndKey_ShouldHaveCorrectValue()
    {
        // Act
        var mode = CosmosAuthenticationMode.AccountEndpointAndKey;

        // Assert
        _ = mode.Should().Be(CosmosAuthenticationMode.AccountEndpointAndKey);
        ((int)mode).Should().Be(1);
    }

    [Fact]
    public void AzureAdCredential_ShouldHaveCorrectValue()
    {
        // Act
        var mode = CosmosAuthenticationMode.AzureAdCredential;

        // Assert
        _ = mode.Should().Be(CosmosAuthenticationMode.AzureAdCredential);
        ((int)mode).Should().Be(2);
    }

    [Fact]
    public void Default_ShouldBeConnectionString()
    {
        // Act
        var defaultMode = default(CosmosAuthenticationMode);

        // Assert
        _ = defaultMode.Should().Be(CosmosAuthenticationMode.ConnectionString);
    }

    [Fact]
    public void AllEnumValues_ShouldBeDefined()
    {
        // Arrange
        var expectedValues = new[]
        {
            CosmosAuthenticationMode.ConnectionString,
            CosmosAuthenticationMode.AccountEndpointAndKey,
            CosmosAuthenticationMode.AzureAdCredential,
        };

        // Act
        var actualValues = Enum.GetValues<CosmosAuthenticationMode>();

        // Assert
        actualValues.Should().BeEquivalentTo(expectedValues);
    }
}

public class CosmosAuthenticationModeExtensionsTests
{
    [Fact]
    public void ToAzureAuthenticationMode_WithConnectionString_ShouldReturnConnectionString()
    {
        // Arrange
        var cosmosMode = CosmosAuthenticationMode.ConnectionString;

        // Act
        var azureMode = cosmosMode.ToAzureAuthenticationMode();

        // Assert
        azureMode.Should().Be(AzureAuthenticationMode.ConnectionString);
    }

    [Fact]
    public void ToAzureAuthenticationMode_WithAccountEndpointAndKey_ShouldReturnEndpointWithKey()
    {
        // Arrange
        var cosmosMode = CosmosAuthenticationMode.AccountEndpointAndKey;

        // Act
        var azureMode = cosmosMode.ToAzureAuthenticationMode();

        // Assert
        azureMode.Should().Be(AzureAuthenticationMode.EndpointWithKey);
    }

    [Fact]
    public void ToAzureAuthenticationMode_WithAzureAdCredential_ShouldReturnAzureAdCredential()
    {
        // Arrange
        var cosmosMode = CosmosAuthenticationMode.AzureAdCredential;

        // Act
        var azureMode = cosmosMode.ToAzureAuthenticationMode();

        // Assert
        azureMode.Should().Be(AzureAuthenticationMode.AzureAdCredential);
    }

    [Fact]
    public void ToAzureAuthenticationMode_WithInvalidValue_ShouldThrowArgumentOutOfRangeException()
    {
        // Arrange
        var invalidMode = (CosmosAuthenticationMode)99;

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => invalidMode.ToAzureAuthenticationMode());
    }

    [Fact]
    public void ToCosmosAuthenticationMode_WithConnectionString_ShouldReturnConnectionString()
    {
        // Arrange
        var azureMode = AzureAuthenticationMode.ConnectionString;

        // Act
        var cosmosMode = azureMode.ToCosmosAuthenticationMode();

        // Assert
        cosmosMode.Should().Be(CosmosAuthenticationMode.ConnectionString);
    }

    [Fact]
    public void ToCosmosAuthenticationMode_WithEndpointWithKey_ShouldReturnAccountEndpointAndKey()
    {
        // Arrange
        var azureMode = AzureAuthenticationMode.EndpointWithKey;

        // Act
        var cosmosMode = azureMode.ToCosmosAuthenticationMode();

        // Assert
        cosmosMode.Should().Be(CosmosAuthenticationMode.AccountEndpointAndKey);
    }

    [Fact]
    public void ToCosmosAuthenticationMode_WithAzureAdCredential_ShouldReturnAzureAdCredential()
    {
        // Arrange
        var azureMode = AzureAuthenticationMode.AzureAdCredential;

        // Act
        var cosmosMode = azureMode.ToCosmosAuthenticationMode();

        // Assert
        cosmosMode.Should().Be(CosmosAuthenticationMode.AzureAdCredential);
    }

    [Fact]
    public void ToCosmosAuthenticationMode_WithInvalidValue_ShouldThrowArgumentOutOfRangeException()
    {
        // Arrange
        var invalidMode = (AzureAuthenticationMode)99;

        // Act & Assert
        Assert.Throws<ArgumentOutOfRangeException>(() => invalidMode.ToCosmosAuthenticationMode());
    }

    [Theory]
    [InlineData(CosmosAuthenticationMode.ConnectionString, AzureAuthenticationMode.ConnectionString)]
    [InlineData(CosmosAuthenticationMode.AccountEndpointAndKey, AzureAuthenticationMode.EndpointWithKey)]
    [InlineData(CosmosAuthenticationMode.AzureAdCredential, AzureAuthenticationMode.AzureAdCredential)]
    public void Conversion_ShouldBeBidirectional(CosmosAuthenticationMode cosmosMode, AzureAuthenticationMode expectedAzureMode)
    {
        // Act
        var convertedToAzure = cosmosMode.ToAzureAuthenticationMode();
        var convertedBackToCosmos = convertedToAzure.ToCosmosAuthenticationMode();

        // Assert
        convertedToAzure.Should().Be(expectedAzureMode);
        convertedBackToCosmos.Should().Be(cosmosMode);
    }
}
