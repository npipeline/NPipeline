using FluentAssertions;
using NPipeline.Connectors.Configuration;
using Xunit;

namespace NPipeline.Connectors.Tests.Configuration;

public class CheckpointStrategyTests
{
    [Fact]
    public void None_HasCorrectValue()
    {
        // Act & Assert
        ((int)CheckpointStrategy.None).Should().Be(0);
    }

    [Fact]
    public void Offset_HasCorrectValue()
    {
        // Act & Assert
        ((int)CheckpointStrategy.Offset).Should().Be(1);
    }

    [Fact]
    public void KeyBased_HasCorrectValue()
    {
        // Act & Assert
        ((int)CheckpointStrategy.KeyBased).Should().Be(2);
    }

    [Fact]
    public void Cursor_HasCorrectValue()
    {
        // Act & Assert
        ((int)CheckpointStrategy.Cursor).Should().Be(3);
    }

    [Fact]
    public void CDC_HasCorrectValue()
    {
        // Act & Assert
        ((int)CheckpointStrategy.CDC).Should().Be(4);
    }

    [Fact]
    public void InMemory_HasCorrectValue()
    {
        // Act & Assert
        ((int)CheckpointStrategy.InMemory).Should().Be(5);
    }

    [Fact]
    public void AllValues_AreUnique()
    {
        // Arrange
        var values = new[]
        {
            CheckpointStrategy.None, CheckpointStrategy.Offset, CheckpointStrategy.KeyBased, CheckpointStrategy.Cursor, CheckpointStrategy.CDC,
            CheckpointStrategy.InMemory,
        };

        // Act & Assert
        values.Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void UnderlyingType_IsInt32()
    {
        // Act & Assert
        Enum.GetUnderlyingType(typeof(CheckpointStrategy)).Should().Be<int>();
    }

    [Theory]
    [InlineData(0, CheckpointStrategy.None)]
    [InlineData(1, CheckpointStrategy.Offset)]
    [InlineData(2, CheckpointStrategy.KeyBased)]
    [InlineData(3, CheckpointStrategy.Cursor)]
    [InlineData(4, CheckpointStrategy.CDC)]
    [InlineData(5, CheckpointStrategy.InMemory)]
    public void CanCastFromInt(int value, CheckpointStrategy expected)
    {
        // Act
        var result = (CheckpointStrategy)value;

        // Assert
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData("None", CheckpointStrategy.None)]
    [InlineData("Offset", CheckpointStrategy.Offset)]
    [InlineData("KeyBased", CheckpointStrategy.KeyBased)]
    [InlineData("Cursor", CheckpointStrategy.Cursor)]
    [InlineData("CDC", CheckpointStrategy.CDC)]
    [InlineData("InMemory", CheckpointStrategy.InMemory)]
    public void CanParseFromString(string value, CheckpointStrategy expected)
    {
        // Act
        var result = Enum.Parse<CheckpointStrategy>(value);

        // Assert
        result.Should().Be(expected);
    }

    [Fact]
    public void Names_AreCorrect()
    {
        // Act & Assert
        nameof(CheckpointStrategy.None).Should().Be("None");
        nameof(CheckpointStrategy.Offset).Should().Be("Offset");
        nameof(CheckpointStrategy.KeyBased).Should().Be("KeyBased");
        nameof(CheckpointStrategy.Cursor).Should().Be("Cursor");
        nameof(CheckpointStrategy.CDC).Should().Be("CDC");
        nameof(CheckpointStrategy.InMemory).Should().Be("InMemory");
    }
}
