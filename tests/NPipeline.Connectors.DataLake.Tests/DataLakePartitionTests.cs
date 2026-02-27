using System.Linq.Expressions;
using NPipeline.Connectors.DataLake.Partitioning;

namespace NPipeline.Connectors.DataLake.Tests;

public sealed class DataLakePartitionTests
{
    #region PartitionSpec Fluent Builder Tests

    [Fact]
    public void PartitionSpec_By_CreatesSpecWithSingleColumn()
    {
        // Arrange & Act
        var spec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate);

        // Assert
        spec.HasPartitions.Should().BeTrue();
        spec.Columns.Should().HaveCount(1);
        spec.Columns[0].ColumnName.Should().Be("event_date");
        spec.Columns[0].PropertyName.Should().Be("EventDate");
        spec.Columns[0].ValueType.Should().Be(typeof(DateOnly));
    }

    [Fact]
    public void PartitionSpec_By_WithCustomColumnName_UsesCustomName()
    {
        // Arrange & Act
        var spec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate, "dt");

        // Assert
        spec.Columns[0].ColumnName.Should().Be("dt");
    }

    [Fact]
    public void PartitionSpec_ThenBy_AddsAdditionalColumn()
    {
        // Arrange & Act
        var spec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region);

        // Assert
        spec.Columns.Should().HaveCount(2);
        spec.Columns[0].ColumnName.Should().Be("event_date");
        spec.Columns[1].ColumnName.Should().Be("region");
    }

    [Fact]
    public void PartitionSpec_ThenBy_WithCustomColumnName_UsesCustomName()
    {
        // Arrange & Act
        var spec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region, "geo");

        // Assert
        spec.Columns[1].ColumnName.Should().Be("geo");
    }

    [Fact]
    public void PartitionSpec_None_CreatesEmptySpec()
    {
        // Arrange & Act
        var spec = PartitionSpec<SalesRecord>.None();

        // Assert
        spec.HasPartitions.Should().BeFalse();
        spec.Columns.Should().BeEmpty();
    }

    [Fact]
    public void PartitionSpec_By_WithNullExpression_ThrowsArgumentNullException()
    {
        // Arrange
        Expression<Func<SalesRecord, DateOnly>>? expression = null;

        // Act
        var act = () => PartitionSpec<SalesRecord>.By(expression!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void PartitionSpec_ThenBy_WithNullExpression_ThrowsArgumentNullException()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.By(x => x.EventDate);
        Expression<Func<SalesRecord, string>>? expression = null;

        // Act
        var act = () => spec.ThenBy(expression!);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    #endregion

    #region Partition Path Builder Tests

    [Fact]
    public void PartitionPathBuilder_BuildPath_WithSinglePartition_ProducesHiveStylePath()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.By(x => x.EventDate);
        var record = new SalesRecord
        {
            Id = 1,
            EventDate = new DateOnly(2025, 1, 15),
            Region = "EU"
        };

        // Act
        var path = PartitionPathBuilder.BuildPath(record, spec);

        // Assert
        path.Should().Be("event_date=2025-01-15/");
    }

    [Fact]
    public void PartitionPathBuilder_BuildPath_WithMultiplePartitions_ProducesNestedPath()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region);

        var record = new SalesRecord
        {
            Id = 1,
            EventDate = new DateOnly(2025, 1, 15),
            Region = "EU"
        };

        // Act
        var path = PartitionPathBuilder.BuildPath(record, spec);

        // Assert
        path.Should().Be("event_date=2025-01-15/region=EU/");
    }

    [Fact]
    public void PartitionPathBuilder_BuildPath_WithEmptySpec_ReturnsEmptyString()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.None();
        var record = new SalesRecord { Id = 1 };

        // Act
        var path = PartitionPathBuilder.BuildPath(record, spec);

        // Assert
        path.Should().BeEmpty();
    }

    [Fact]
    public void PartitionPathBuilder_BuildPath_WithNullSpec_ThrowsArgumentNullException()
    {
        // Arrange
        var record = new SalesRecord { Id = 1 };
        PartitionSpec<SalesRecord> spec = null!;

        // Act
        var act = () => PartitionPathBuilder.BuildPath(record, spec);

        // Assert
        act.Should().Throw<ArgumentNullException>();
    }

    #endregion

    #region Data Type Formatting Tests

    [Fact]
    public void PartitionPathBuilder_FormatPartitionValue_WithDateOnly_FormatsAsIsoDate()
    {
        // Arrange
        var date = new DateOnly(2025, 1, 15);

        // Act
        var result = PartitionPathBuilder.FormatPartitionValue(date, typeof(DateOnly));

        // Assert
        result.Should().Be("2025-01-15");
    }

    [Fact]
    public void PartitionPathBuilder_FormatPartitionValue_WithDateTime_FormatsAsIsoDateTime()
    {
        // Arrange
        var dateTime = new DateTime(2025, 1, 15, 10, 30, 45, DateTimeKind.Utc);

        // Act
        var result = PartitionPathBuilder.FormatPartitionValue(dateTime, typeof(DateTime));

        // Assert
        result.Should().Be("2025-01-15-10-30-45");
    }

    [Fact]
    public void PartitionPathBuilder_FormatPartitionValue_WithString_UrlEncodesValue()
    {
        // Arrange
        var value = "hello world";

        // Act
        var result = PartitionPathBuilder.FormatPartitionValue(value, typeof(string));

        // Assert
        result.Should().Be("hello%20world");
    }

    [Fact]
    public void PartitionPathBuilder_FormatPartitionValue_WithEnum_FormatsAsLowercase()
    {
        // Arrange
        var value = Status.Active;

        // Act
        var result = PartitionPathBuilder.FormatPartitionValue(value, typeof(Status));

        // Assert
        result.Should().Be("active");
    }

    [Fact]
    public void PartitionPathBuilder_FormatPartitionValue_WithInt_FormatsAsInvariantString()
    {
        // Arrange
        var value = 42;

        // Act
        var result = PartitionPathBuilder.FormatPartitionValue(value, typeof(int));

        // Assert
        result.Should().Be("42");
    }

    [Fact]
    public void PartitionPathBuilder_FormatPartitionValue_WithGuid_FormatsAsLowercaseDFormat()
    {
        // Arrange
        var guid = Guid.Parse("A1B2C3D4-E5F6-7890-ABCD-EF1234567890");

        // Act
        var result = PartitionPathBuilder.FormatPartitionValue(guid, typeof(Guid));

        // Assert
        result.Should().Be("a1b2c3d4-e5f6-7890-abcd-ef1234567890");
    }

    [Fact]
    public void PartitionPathBuilder_FormatPartitionValue_WithNull_ReturnsNullString()
    {
        // Arrange
        object? value = null;

        // Act
        var result = PartitionPathBuilder.FormatPartitionValue(value, typeof(string));

        // Assert
        result.Should().Be("null");
    }

    #endregion

    #region Partition Key Tests

    [Fact]
    public void PartitionKey_ToHiveStylePath_ProducesCorrectFormat()
    {
        // Arrange
        var key = new PartitionKey
        {
            ColumnName = "event_date",
            Value = "2025-01-15"
        };

        // Act
        var path = key.ToHiveStylePath();

        // Assert
        path.Should().Be("event_date=2025-01-15/");
    }

    [Fact]
    public void PartitionKey_Parse_ParsesValidHiveStyleString()
    {
        // Arrange
        var hiveString = "event_date=2025-01-15";

        // Act
        var key = PartitionKey.Parse(hiveString);

        // Assert
        key.ColumnName.Should().Be("event_date");
        key.Value.Should().Be("2025-01-15");
    }

    [Fact]
    public void PartitionKey_TryParse_WithValidString_ReturnsTrue()
    {
        // Arrange
        var hiveString = "region=EU";

        // Act
        var success = PartitionKey.TryParse(hiveString, out var key);

        // Assert
        success.Should().BeTrue();
        key.Should().NotBeNull();
        key!.ColumnName.Should().Be("region");
        key.Value.Should().Be("EU");
    }

    [Fact]
    public void PartitionKey_TryParse_WithInvalidString_ReturnsFalse()
    {
        // Arrange
        var invalidString = "invalid_format";

        // Act
        var success = PartitionKey.TryParse(invalidString, out var key);

        // Assert
        success.Should().BeFalse();
        key.Should().BeNull();
    }

    [Fact]
    public void PartitionKey_TryParse_WithNullOrEmpty_ReturnsFalse()
    {
        // Act & Assert
        PartitionKey.TryParse(null, out var key1).Should().BeFalse();
        PartitionKey.TryParse("", out var key2).Should().BeFalse();
    }

    #endregion

    #region Extract Partition Keys Tests

    [Fact]
    public void PartitionPathBuilder_ExtractPartitionKeys_ReturnsCorrectKeys()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>
            .By(x => x.EventDate)
            .ThenBy(x => x.Region);

        var record = new SalesRecord
        {
            EventDate = new DateOnly(2025, 1, 15),
            Region = "EU"
        };

        // Act
        var keys = PartitionPathBuilder.ExtractPartitionKeys(record, spec);

        // Assert
        keys.Should().HaveCount(2);
        keys[0].ColumnName.Should().Be("event_date");
        keys[0].Value.Should().Be("2025-01-15");
        keys[1].ColumnName.Should().Be("region");
        keys[1].Value.Should().Be("EU");
    }

    [Fact]
    public void PartitionPathBuilder_ExtractPartitionKeys_WithEmptySpec_ReturnsEmptyList()
    {
        // Arrange
        var spec = PartitionSpec<SalesRecord>.None();
        var record = new SalesRecord { Id = 1 };

        // Act
        var keys = PartitionPathBuilder.ExtractPartitionKeys(record, spec);

        // Assert
        keys.Should().BeEmpty();
    }

    #endregion

    #region Parse Path Tests

    [Fact]
    public void PartitionPathBuilder_ParsePath_ParsesValidPath()
    {
        // Arrange
        var path = "event_date=2025-01-15/region=EU/";

        // Act
        var keys = PartitionPathBuilder.ParsePath(path);

        // Assert
        keys.Should().HaveCount(2);
        keys[0].ColumnName.Should().Be("event_date");
        keys[0].Value.Should().Be("2025-01-15");
        keys[1].ColumnName.Should().Be("region");
        keys[1].Value.Should().Be("EU");
    }

    [Fact]
    public void PartitionPathBuilder_ParsePath_WithEmptyPath_ReturnsEmptyList()
    {
        // Act
        var keys = PartitionPathBuilder.ParsePath("");

        // Assert
        keys.Should().BeEmpty();
    }

    #endregion

    #region Test Record Types

    private sealed class SalesRecord
    {
        public int Id { get; set; }
        public string ProductName { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public DateOnly EventDate { get; set; }
        public string Region { get; set; } = string.Empty;
    }

    private enum Status
    {
        Active,
        Inactive,
        Pending
    }

    #endregion
}
