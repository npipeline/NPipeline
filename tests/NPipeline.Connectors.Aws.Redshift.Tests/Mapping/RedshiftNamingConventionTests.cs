using NPipeline.Connectors.Aws.Redshift.Mapping;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Mapping;

public class RedshiftNamingConventionTests
{
    [Fact]
    public void Convert_PascalCase_ToSnakeCase()
    {
        // Arrange
        var convention = RedshiftNamingConvention.PascalToSnakeCase;

        // Act
        var result = RedshiftMapperBuilder.GetColumnName(
            typeof(TestClass).GetProperty(nameof(TestClass.OrderDate))!, convention);

        // Assert
        result.Should().Be("order_date");
    }

    [Fact]
    public void Convert_Acronyms_ToSnakeCase()
    {
        // Arrange
        var convention = RedshiftNamingConvention.PascalToSnakeCase;

        // Act
        var orderId = RedshiftMapperBuilder.GetColumnName(
            typeof(TestClass).GetProperty(nameof(TestClass.OrderID))!, convention);

        var httpStatus = RedshiftMapperBuilder.GetColumnName(
            typeof(TestClass).GetProperty(nameof(TestClass.HTTPStatusCode))!, convention);

        // Assert
        orderId.Should().Be("order_id");
        httpStatus.Should().Be("http_status_code");
    }

    [Fact]
    public void Convert_SingleWord_ToSnakeCase()
    {
        // Arrange
        var convention = RedshiftNamingConvention.PascalToSnakeCase;

        // Act
        var result = RedshiftMapperBuilder.GetColumnName(
            typeof(TestClass).GetProperty(nameof(TestClass.Name))!, convention);

        // Assert
        result.Should().Be("name");
    }

    [Fact]
    public void Convert_AlreadySnakeCase_IsIdempotent()
    {
        // This tests that the conversion handles already snake_case input gracefully
        // Arrange
        var convention = RedshiftNamingConvention.PascalToSnakeCase;

        // Act
        var result = RedshiftMapperBuilder.GetColumnName(
            typeof(TestClass).GetProperty(nameof(TestClass.order_date))!, convention);

        // Assert
        result.Should().Be("order_date");
    }

    [Fact]
    public void Convert_AsIs_DoesNotTransform()
    {
        // Arrange
        var convention = RedshiftNamingConvention.AsIs;

        // Act
        var result = RedshiftMapperBuilder.GetColumnName(
            typeof(TestClass).GetProperty(nameof(TestClass.OrderDate))!, convention);

        // Assert
        result.Should().Be("OrderDate");
    }

    [Fact]
    public void Convert_Lowercase_AllLower()
    {
        // Arrange
        var convention = RedshiftNamingConvention.Lowercase;

        // Act
        var result = RedshiftMapperBuilder.GetColumnName(
            typeof(TestClass).GetProperty(nameof(TestClass.OrderDate))!, convention);

        // Assert
        result.Should().Be("orderdate");
    }

    [Fact]
    public void Attribute_OverridesConvention()
    {
        // Arrange
        var convention = RedshiftNamingConvention.PascalToSnakeCase;

        // Act
        var result = RedshiftMapperBuilder.GetColumnName(
            typeof(TestClass).GetProperty(nameof(TestClass.CustomColumn))!, convention);

        // Assert
        result.Should().Be("custom_column_name");
    }

    private sealed class TestClass
    {
        public string OrderDate { get; set; } = string.Empty;
        public string OrderID { get; set; } = string.Empty;
        public string HTTPStatusCode { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public string order_date { get; set; } = string.Empty;

        [RedshiftColumn("custom_column_name")]
        public string CustomColumn { get; set; } = string.Empty;
    }
}
