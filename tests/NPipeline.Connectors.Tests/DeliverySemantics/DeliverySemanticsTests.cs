using AwesomeAssertions;
using Xunit;
using DeliverySemanticsEnum = NPipeline.Connectors.DeliverySemantics.DeliverySemantics;

namespace NPipeline.Connectors.Tests.DeliverySemantics;

public class DeliverySemanticsTests
{
    [Fact]
    public void AtLeastOnce_HasCorrectValue()
    {
        // Act & Assert
        ((byte)DeliverySemanticsEnum.AtLeastOnce).Should().Be(0);
    }

    [Fact]
    public void AtMostOnce_HasCorrectValue()
    {
        // Act & Assert
        ((byte)DeliverySemanticsEnum.AtMostOnce).Should().Be(1);
    }

    [Fact]
    public void ExactlyOnce_HasCorrectValue()
    {
        // Act & Assert
        ((byte)DeliverySemanticsEnum.ExactlyOnce).Should().Be(2);
    }

    [Fact]
    public void AllValues_AreUnique()
    {
        // Arrange
        var values = new[] { DeliverySemanticsEnum.AtLeastOnce, DeliverySemanticsEnum.AtMostOnce, DeliverySemanticsEnum.ExactlyOnce };

        // Act & Assert
        values.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void UnderlyingType_IsByte()
    {
        // Act & Assert
        Enum.GetUnderlyingType(typeof(DeliverySemanticsEnum)).Should().Be<byte>();
    }

    [Theory]
    [InlineData(0, DeliverySemanticsEnum.AtLeastOnce)]
    [InlineData(1, DeliverySemanticsEnum.AtMostOnce)]
    [InlineData(2, DeliverySemanticsEnum.ExactlyOnce)]
    public void CanCastFromByte(byte value, DeliverySemanticsEnum expected)
    {
        // Act
        var result = (DeliverySemanticsEnum)value;

        // Assert
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("AtLeastOnce", DeliverySemanticsEnum.AtLeastOnce)]
    [InlineData("AtMostOnce", DeliverySemanticsEnum.AtMostOnce)]
    [InlineData("ExactlyOnce", DeliverySemanticsEnum.ExactlyOnce)]
    public void CanParseFromString(string value, DeliverySemanticsEnum expected)
    {
        // Act
        var result = Enum.Parse<DeliverySemanticsEnum>(value);

        // Assert
        result.Should().Be(expected);
    }

    [Fact]
    public void Names_AreCorrect()
    {
        // Act & Assert
        nameof(DeliverySemanticsEnum.AtLeastOnce).Should().Be("AtLeastOnce");
        nameof(DeliverySemanticsEnum.AtMostOnce).Should().Be("AtMostOnce");
        nameof(DeliverySemanticsEnum.ExactlyOnce).Should().Be("ExactlyOnce");
    }
}
