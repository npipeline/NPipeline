using NPipeline.Connectors.Aws.Redshift.Exceptions;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Exceptions;

public class RedshiftMappingExceptionTests
{
    [Fact]
    public void DefaultConstructor_CreatesExceptionWithDefaultMessage()
    {
        // Act
        var exception = new RedshiftMappingException();

        // Assert
        exception.Message.Should().Contain("mapping");
    }

    [Fact]
    public void MessageConstructor_CreatesExceptionWithMessage()
    {
        // Arrange
        const string message = "Mapping failed";

        // Act
        var exception = new RedshiftMappingException(message);

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void InnerExceptionConstructor_CreatesExceptionWithInnerException()
    {
        // Arrange
        const string message = "Mapping failed";
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = new RedshiftMappingException(message, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.InnerException.Should().Be(innerException);
    }

    [Fact]
    public void FullConstructor_SetsAllProperties()
    {
        // Arrange
        const string message = "Mapping failed";
        var mappedType = typeof(TestEntity);
        const string propertyName = "Name";
        const string columnName = "user_name";
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = new RedshiftMappingException(
            message,
            mappedType,
            propertyName,
            columnName,
            innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.MappedType.Should().Be(mappedType);
        exception.PropertyName.Should().Be(propertyName);
        exception.ColumnName.Should().Be(columnName);
        exception.InnerException.Should().Be(innerException);
    }

    [Fact]
    public void ToString_WithType_IncludesTypeInOutput()
    {
        // Arrange
        var exception = new RedshiftMappingException(
            "Mapping failed",
            typeof(TestEntity));

        // Act
        var result = exception.ToString();

        // Assert
        result.Should().Contain("Type: TestEntity");
    }

    [Fact]
    public void ToString_WithPropertyName_IncludesPropertyInOutput()
    {
        // Arrange
        var exception = new RedshiftMappingException(
            "Mapping failed",
            propertyName: "Name");

        // Act
        var result = exception.ToString();

        // Assert
        result.Should().Contain("Property: Name");
    }

    [Fact]
    public void ToString_WithColumnName_IncludesColumnInOutput()
    {
        // Arrange
        var exception = new RedshiftMappingException(
            "Mapping failed",
            columnName: "user_name");

        // Act
        var result = exception.ToString();

        // Assert
        result.Should().Contain("Column: user_name");
    }

    [Fact]
    public void ToString_WithAllProperties_IncludesAllInOutput()
    {
        // Arrange
        var exception = new RedshiftMappingException(
            "Mapping failed",
            typeof(TestEntity),
            "Name",
            "user_name");

        // Act
        var result = exception.ToString();

        // Assert
        result.Should().Contain("Type: TestEntity");
        result.Should().Contain("Property: Name");
        result.Should().Contain("Column: user_name");
    }

    private sealed class TestEntity
    {
        public string? Name { get; set; }
    }
}
