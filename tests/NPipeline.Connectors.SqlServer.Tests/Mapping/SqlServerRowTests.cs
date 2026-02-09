using AwesomeAssertions;
using FakeItEasy;
using NPipeline.Connectors.SqlServer.Mapping;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.SqlServer.Tests.Mapping;

public sealed class SqlServerRowTests
{
    [Fact]
    public void Constructor_WithValidReader_CreatesRow()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(3);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.GetName(1)).Returns("Name");
        A.CallTo(() => reader.GetName(2)).Returns("Value");

        // Act
        var row = new SqlServerRow(reader);

        // Assert
        _ = row.Should().NotBeNull();
        _ = row.FieldCount.Should().Be(3);
    }

    [Fact]
    public void Constructor_WithNullReader_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new SqlServerRow(null!));
    }

    [Fact]
    public void Constructor_WithCaseInsensitiveFalse_UsesOrdinalComparison()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");

        // Act
        var row = new SqlServerRow(reader, false);

        // Assert
        _ = row.Should().NotBeNull();
    }

    [Fact]
    public void FieldCount_ReturnsReaderFieldCount()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(5);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.GetName(1)).Returns("Name");
        A.CallTo(() => reader.GetName(2)).Returns("Value");
        A.CallTo(() => reader.GetName(3)).Returns("Created");
        A.CallTo(() => reader.GetName(4)).Returns("Active");

        var row = new SqlServerRow(reader);

        // Act
        var fieldCount = row.FieldCount;

        // Assert
        _ = fieldCount.Should().Be(5);
    }

    [Fact]
    public void ColumnNames_ReturnsAllColumnNames()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(3);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.GetName(1)).Returns("Name");
        A.CallTo(() => reader.GetName(2)).Returns("Value");

        var row = new SqlServerRow(reader);

        // Act
        var columnNames = row.ColumnNames;

        // Assert
        _ = columnNames.Should().HaveCount(3);
        _ = columnNames[0].Should().Be("Id");
        _ = columnNames[1].Should().Be("Name");
        _ = columnNames[2].Should().Be("Value");
    }

    [Fact]
    public void GetName_ReturnsColumnName()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(2);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.GetName(1)).Returns("Name");

        var row = new SqlServerRow(reader);

        // Act
        var name = row.GetName(1);

        // Assert
        _ = name.Should().Be("Name");
    }

    [Fact]
    public void GetFieldType_ReturnsFieldType()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.GetFieldType(0)).Returns(typeof(int));

        var row = new SqlServerRow(reader);

        // Act
        var fieldType = row.GetFieldType(0);

        // Assert
        _ = fieldType.Should().Be<int>();
    }

    [Fact]
    public void HasColumn_WithExistingColumn_ReturnsTrue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(2);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.GetName(1)).Returns("Name");

        var row = new SqlServerRow(reader);

        // Act
        var hasColumn = row.HasColumn("Name");

        // Assert
        _ = hasColumn.Should().BeTrue();
    }

    [Fact]
    public void HasColumn_WithNonExistingColumn_ReturnsFalse()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(2);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.GetName(1)).Returns("Name");

        var row = new SqlServerRow(reader);

        // Act
        var hasColumn = row.HasColumn("NonExisting");

        // Assert
        _ = hasColumn.Should().BeFalse();
    }

    [Fact]
    public void Get_WithExistingColumn_ReturnsValue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(false);
        A.CallTo(() => reader.GetFieldValue<int>(0)).Returns(42);

        var row = new SqlServerRow(reader);

        // Act
        var value = row.Get<int>("Id");

        // Assert
        _ = value.Should().Be(42);
    }

    [Fact]
    public void Get_WithNullValue_ReturnsDefault()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(true);

        var row = new SqlServerRow(reader);

        // Act
        var value = row.Get<int>("Id");

        // Assert
        _ = value.Should().Be(0);
    }

    [Fact]
    public void Get_WithDefaultValue_ReturnsDefaultWhenNull()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(true);

        var row = new SqlServerRow(reader);

        // Act
        var value = row.Get("Id", 99);

        // Assert
        _ = value.Should().Be(99);
    }

    [Fact]
    public void Get_WithOrdinal_ReturnsValue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(false);
        A.CallTo(() => reader.GetFieldValue<int>(0)).Returns(42);

        var row = new SqlServerRow(reader);

        // Act
        var value = row.Get<int>(0);

        // Assert
        _ = value.Should().Be(42);
    }

    [Fact]
    public void TryGet_WithExistingColumn_ReturnsTrueAndValue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(false);
        A.CallTo(() => reader.GetFieldValue<int>(0)).Returns(42);

        var row = new SqlServerRow(reader);

        // Act
        var result = row.TryGet<int>("Id", out var value);

        // Assert
        _ = result.Should().BeTrue();
        _ = value.Should().Be(42);
    }

    [Fact]
    public void TryGet_WithNonExistingColumn_ReturnsFalseAndDefault()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");

        var row = new SqlServerRow(reader);

        // Act
        var result = row.TryGet<int>("NonExisting", out var value);

        // Assert
        _ = result.Should().BeFalse();
        _ = value.Should().Be(0);
    }

    [Fact]
    public void TryGet_WithNullValue_ReturnsFalseAndDefault()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(true);

        var row = new SqlServerRow(reader);

        // Act
        var result = row.TryGet<int>("Id", out var value);

        // Assert
        _ = result.Should().BeFalse();
        _ = value.Should().Be(0);
    }

    [Fact]
    public void TryGet_WithOrdinal_ReturnsTrueAndValue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(false);
        A.CallTo(() => reader.GetFieldValue<int>(0)).Returns(42);

        var row = new SqlServerRow(reader);

        // Act
        var result = row.TryGet<int>(0, out var value);

        // Assert
        _ = result.Should().BeTrue();
        _ = value.Should().Be(42);
    }

    [Fact]
    public void GetValue_WithExistingColumn_ReturnsValue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(false);
        A.CallTo(() => reader.GetFieldValue<object>(0)).Returns(42);

        var row = new SqlServerRow(reader);

        // Act
        var value = row.GetValue("Id");

        // Assert
        _ = value.Should().Be(42);
    }

    [Fact]
    public void GetValue_WithNonExistingColumn_ReturnsNull()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");

        var row = new SqlServerRow(reader);

        // Act
        var value = row.GetValue("NonExisting");

        // Assert
        _ = value.Should().BeNull();
    }

    [Fact]
    public void GetValue_WithOrdinal_ReturnsValue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(false);
        A.CallTo(() => reader.GetFieldValue<object>(0)).Returns(42);

        var row = new SqlServerRow(reader);

        // Act
        var value = row.GetValue(0);

        // Assert
        _ = value.Should().Be(42);
    }

    [Fact]
    public void IsDBNull_WithNullColumn_ReturnsTrue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(true);

        var row = new SqlServerRow(reader);

        // Act
        var isNull = row.IsDBNull("Id");

        // Assert
        _ = isNull.Should().BeTrue();
    }

    [Fact]
    public void IsDBNull_WithNonNullColumn_ReturnsFalse()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(false);

        var row = new SqlServerRow(reader);

        // Act
        var isNull = row.IsDBNull("Id");

        // Assert
        _ = isNull.Should().BeFalse();
    }

    [Fact]
    public void IsDBNull_WithOrdinal_ReturnsTrue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(true);

        var row = new SqlServerRow(reader);

        // Act
        var isNull = row.IsDBNull(0);

        // Assert
        _ = isNull.Should().BeTrue();
    }

    [Fact]
    public void CaseInsensitiveMapping_AllowsDifferentCaseAccess()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("Id");
        A.CallTo(() => reader.IsDBNull(0)).Returns(false);
        A.CallTo(() => reader.GetFieldValue<int>(0)).Returns(42);

        var row = new SqlServerRow(reader);

        // Act
        var value1 = row.Get<int>("Id");
        var value2 = row.Get<int>("id");
        var value3 = row.Get<int>("ID");

        // Assert
        _ = value1.Should().Be(42);
        _ = value2.Should().Be(42);
        _ = value3.Should().Be(42);
    }
}
