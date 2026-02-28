using FakeItEasy;
using NPipeline.Connectors.Snowflake.Mapping;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Snowflake.Tests.Mapping;

public sealed class SnowflakeRowTests
{
    [Fact]
    public void HasColumn_WithCaseInsensitiveLookup_ShouldReturnTrue()
    {
        // Arrange
        var reader = CreateReader();
        var row = new SnowflakeRow(reader, caseInsensitive: true);

        // Act
        var hasUpper = row.HasColumn("FIRST_NAME");
        var hasLower = row.HasColumn("first_name");

        // Assert
        Assert.True(hasUpper);
        Assert.True(hasLower);
    }

    [Fact]
    public void Get_WithColumnName_ShouldReturnTypedValue()
    {
        // Arrange
        var reader = CreateReader();
        var row = new SnowflakeRow(reader, caseInsensitive: true);

        // Act
        var value = row.Get<string>("FIRST_NAME");

        // Assert
        Assert.Equal("Ada", value);
    }

    [Fact]
    public void TryGet_WithMissingColumn_ShouldReturnFalseAndDefault()
    {
        // Arrange
        var reader = CreateReader();
        var row = new SnowflakeRow(reader);

        // Act
        var found = row.TryGet<int>("MISSING", out var value);

        // Assert
        Assert.False(found);
        Assert.Equal(default, value);
    }

    [Fact]
    public void GetValue_WithDbNull_ShouldReturnNull()
    {
        // Arrange
        var reader = CreateReader();
        var row = new SnowflakeRow(reader);

        // Act
        var value = row.GetValue("LAST_LOGIN");

        // Assert
        Assert.Null(value);
    }

    private static IDatabaseReader CreateReader()
    {
        var reader = A.Fake<IDatabaseReader>();

        A.CallTo(() => reader.FieldCount).Returns(3);

        A.CallTo(() => reader.GetName(0)).Returns("FIRST_NAME");
        A.CallTo(() => reader.GetName(1)).Returns("AGE");
        A.CallTo(() => reader.GetName(2)).Returns("LAST_LOGIN");

        A.CallTo(() => reader.IsDBNull(0)).Returns(false);
        A.CallTo(() => reader.IsDBNull(1)).Returns(false);
        A.CallTo(() => reader.IsDBNull(2)).Returns(true);

        A.CallTo(() => reader.GetFieldValue<string>(0)).Returns("Ada");
        A.CallTo(() => reader.GetFieldValue<int>(1)).Returns(42);
        A.CallTo(() => reader.GetFieldValue<object>(0)).Returns("Ada");
        A.CallTo(() => reader.GetFieldValue<object>(1)).Returns(42);

        return reader;
    }
}
