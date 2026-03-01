using FakeItEasy;
using NPipeline.Connectors.Aws.Redshift.Mapping;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Mapping;

public class RedshiftRowTests
{
    [Fact]
    public void Get_ExistingColumn_WithCorrectType_ReturnsValue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("customer_name");
        A.CallTo(() => reader.IsDBNull(0)).Returns(false);
        A.CallTo(() => reader.GetFieldValue<string>(0)).Returns("John Doe");

        var row = new RedshiftRow(reader);

        // Act
        var result = row.Get<string>("customer_name");

        // Assert
        result.Should().Be("John Doe");
    }

    [Fact]
    public void Get_ExistingColumn_WithNullValue_ReturnsDefault()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("customer_name");
        A.CallTo(() => reader.IsDBNull(0)).Returns(true);

        var row = new RedshiftRow(reader);

        // Act
        var result = row.Get<string>("customer_name");

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void Get_NonExistentColumn_ReturnsDefault()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(0);

        var row = new RedshiftRow(reader);

        // Act
        var result = row.Get<string>("non_existent", "default_value");

        // Assert
        result.Should().Be("default_value");
    }

    [Fact]
    public void HasColumn_CaseInsensitive_ReturnsTrue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("customer_name");

        var row = new RedshiftRow(reader);

        // Act & Assert
        row.HasColumn("CUSTOMER_NAME").Should().BeTrue();
        row.HasColumn("Customer_Name").Should().BeTrue();
        row.HasColumn("customer_name").Should().BeTrue();
    }

    [Fact]
    public void HasColumn_CaseSensitive_ReturnsFalseForDifferentCase()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("customer_name");

        var row = new RedshiftRow(reader, false);

        // Act & Assert
        row.HasColumn("customer_name").Should().BeTrue();
        row.HasColumn("CUSTOMER_NAME").Should().BeFalse();
    }

    [Fact]
    public void Get_ByOrdinal_ReturnsValue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("age");
        A.CallTo(() => reader.IsDBNull(0)).Returns(false);
        A.CallTo(() => reader.GetFieldValue<int>(0)).Returns(42);

        var row = new RedshiftRow(reader);

        // Act
        var result = row.Get<int>(0);

        // Assert
        result.Should().Be(42);
    }

    [Fact]
    public void Get_ByOrdinal_WithNullValue_ReturnsDefault()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("age");
        A.CallTo(() => reader.IsDBNull(0)).Returns(true);

        var row = new RedshiftRow(reader);

        // Act
        var result = row.Get(0, 99);

        // Assert
        result.Should().Be(99);
    }

    [Fact]
    public void IsNull_WhenDBNull_ReturnsTrue()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("nullable_column");
        A.CallTo(() => reader.IsDBNull(0)).Returns(true);

        var row = new RedshiftRow(reader);

        // Act
        var result = row.IsNull("nullable_column");

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public void IsNull_WhenNotNull_ReturnsFalse()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("non_nullable_column");
        A.CallTo(() => reader.IsDBNull(0)).Returns(false);

        var row = new RedshiftRow(reader);

        // Act
        var result = row.IsNull("non_nullable_column");

        // Assert
        result.Should().BeFalse();
    }

    [Fact]
    public void ColumnNames_ReturnsAllColumnNames()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(3);
        A.CallTo(() => reader.GetName(0)).Returns("id");
        A.CallTo(() => reader.GetName(1)).Returns("name");
        A.CallTo(() => reader.GetName(2)).Returns("created_at");

        var row = new RedshiftRow(reader);

        // Act
        var names = row.ColumnNames;

        // Assert
        names.Should().ContainInOrder("id", "name", "created_at");
    }

    [Fact]
    public void FieldCount_ReturnsCorrectCount()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(5);

        var row = new RedshiftRow(reader);

        // Act & Assert
        row.FieldCount.Should().Be(5);
    }

    [Fact]
    public void TryGetOrdinal_ExistingColumn_ReturnsTrueWithCorrectOrdinal()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(3);
        A.CallTo(() => reader.GetName(0)).Returns("id");
        A.CallTo(() => reader.GetName(1)).Returns("name");
        A.CallTo(() => reader.GetName(2)).Returns("email");

        var row = new RedshiftRow(reader);

        // Act
        var result = row.TryGetOrdinal("name", out var ordinal);

        // Assert
        result.Should().BeTrue();
        ordinal.Should().Be(1);
    }

    [Fact]
    public void TryGetOrdinal_NonExistentColumn_ReturnsFalse()
    {
        // Arrange
        var reader = A.Fake<IDatabaseReader>();
        A.CallTo(() => reader.FieldCount).Returns(1);
        A.CallTo(() => reader.GetName(0)).Returns("id");

        var row = new RedshiftRow(reader);

        // Act
        var result = row.TryGetOrdinal("non_existent", out var ordinal);

        // Assert
        result.Should().BeFalse();
    }
}
