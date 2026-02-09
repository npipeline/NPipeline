using AwesomeAssertions;
using NPipeline.Connectors.DeliverySemantics;
using Xunit;

namespace NPipeline.Connectors.Tests.DeliverySemantics;

public class ExactlyOnceModeTests
{
    [Fact]
    public void Transactional_HasCorrectValue()
    {
        // Act & Assert
        ((int)ExactlyOnceMode.Transactional).Should().Be(0);
    }

    [Fact]
    public void TwoPhaseCommit_HasCorrectValue()
    {
        // Act & Assert
        ((int)ExactlyOnceMode.TwoPhaseCommit).Should().Be(1);
    }

    [Fact]
    public void Idempotent_HasCorrectValue()
    {
        // Act & Assert
        ((int)ExactlyOnceMode.Idempotent).Should().Be(2);
    }

    [Fact]
    public void AllValues_AreUnique()
    {
        // Arrange
        var values = new[] { ExactlyOnceMode.Transactional, ExactlyOnceMode.TwoPhaseCommit, ExactlyOnceMode.Idempotent };

        // Act & Assert
        values.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void UnderlyingType_IsInt32()
    {
        // Act & Assert
        Enum.GetUnderlyingType(typeof(ExactlyOnceMode)).Should().Be<int>();
    }

    [Theory]
    [InlineData(0, ExactlyOnceMode.Transactional)]
    [InlineData(1, ExactlyOnceMode.TwoPhaseCommit)]
    [InlineData(2, ExactlyOnceMode.Idempotent)]
    public void CanCastFromInt(int value, ExactlyOnceMode expected)
    {
        // Act
        var result = (ExactlyOnceMode)value;

        // Assert
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("Transactional", ExactlyOnceMode.Transactional)]
    [InlineData("TwoPhaseCommit", ExactlyOnceMode.TwoPhaseCommit)]
    [InlineData("Idempotent", ExactlyOnceMode.Idempotent)]
    public void CanParseFromString(string value, ExactlyOnceMode expected)
    {
        // Act
        var result = Enum.Parse<ExactlyOnceMode>(value);

        // Assert
        result.Should().Be(expected);
    }

    [Fact]
    public void Names_AreCorrect()
    {
        // Act & Assert
        nameof(ExactlyOnceMode.Transactional).Should().Be("Transactional");
        nameof(ExactlyOnceMode.TwoPhaseCommit).Should().Be("TwoPhaseCommit");
        nameof(ExactlyOnceMode.Idempotent).Should().Be("Idempotent");
    }
}
