using FluentAssertions;
using NPipeline.Connectors.Configuration;
using Xunit;

namespace NPipeline.Connectors.Tests.Configuration;

public class DeliverySemanticTests
{
    [Fact]
    public void AtLeastOnce_HasCorrectValue()
    {
        // Act & Assert
        ((int)DeliverySemantic.AtLeastOnce).Should().Be(0);
    }

    [Fact]
    public void AtMostOnce_HasCorrectValue()
    {
        // Act & Assert
        ((int)DeliverySemantic.AtMostOnce).Should().Be(1);
    }

    [Fact]
    public void ExactlyOnce_HasCorrectValue()
    {
        // Act & Assert
        ((int)DeliverySemantic.ExactlyOnce).Should().Be(2);
    }

    [Fact]
    public void AllValues_AreUnique()
    {
        // Arrange
        var values = new[] { DeliverySemantic.AtLeastOnce, DeliverySemantic.AtMostOnce, DeliverySemantic.ExactlyOnce };

        // Act & Assert
        values.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void UnderlyingType_IsInt32()
    {
        // Act & Assert
        Enum.GetUnderlyingType(typeof(DeliverySemantic)).Should().Be<int>();
    }

    [Theory]
    [InlineData(0, DeliverySemantic.AtLeastOnce)]
    [InlineData(1, DeliverySemantic.AtMostOnce)]
    [InlineData(2, DeliverySemantic.ExactlyOnce)]
    public void CanCastFromInt(int value, DeliverySemantic expected)
    {
        // Act
        var result = (DeliverySemantic)value;

        // Assert
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("AtLeastOnce", DeliverySemantic.AtLeastOnce)]
    [InlineData("AtMostOnce", DeliverySemantic.AtMostOnce)]
    [InlineData("ExactlyOnce", DeliverySemantic.ExactlyOnce)]
    public void CanParseFromString(string value, DeliverySemantic expected)
    {
        // Act
        var result = Enum.Parse<DeliverySemantic>(value);

        // Assert
        result.Should().Be(expected);
    }

    [Fact]
    public void Names_AreCorrect()
    {
        // Act & Assert
        nameof(DeliverySemantic.AtLeastOnce).Should().Be("AtLeastOnce");
        nameof(DeliverySemantic.AtMostOnce).Should().Be("AtMostOnce");
        nameof(DeliverySemantic.ExactlyOnce).Should().Be("ExactlyOnce");
    }
}
