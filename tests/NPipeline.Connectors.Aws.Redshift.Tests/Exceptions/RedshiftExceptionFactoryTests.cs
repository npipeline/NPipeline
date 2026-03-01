using NPipeline.Connectors.Aws.Redshift.Exceptions;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Exceptions;

public class RedshiftExceptionFactoryTests
{
    [Fact]
    public void Create_WithNullException_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => RedshiftExceptionFactory.Create(null!));
    }

    [Fact]
    public void CreateMappingException_CreatesExceptionWithAllProperties()
    {
        // Arrange
        const string message = "Mapping failed";
        var mappedType = typeof(TestEntity);
        const string propertyName = "Name";
        const string columnName = "user_name";
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = RedshiftExceptionFactory.CreateMappingException(
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
    public void CreateMappingException_WithMinimalParameters_CreatesException()
    {
        // Arrange
        const string message = "Mapping failed";

        // Act
        var exception = RedshiftExceptionFactory.CreateMappingException(message);

        // Assert
        exception.Message.Should().Be(message);
        exception.MappedType.Should().BeNull();
        exception.PropertyName.Should().BeNull();
        exception.ColumnName.Should().BeNull();

        // Note: InnerException is not null because the base class requires a non-null inner exception
    }

    [Fact]
    public void CreateConnectionException_CreatesExceptionWithConnectionString()
    {
        // Arrange
        const string message = "Connection failed";
        const string connectionString = "Host=localhost;Database=test;Password=secret";

        // Act
        var exception = RedshiftExceptionFactory.CreateConnectionException(message, connectionString);

        // Assert
        exception.Message.Should().Be(message);
        exception.ConnectionString.Should().NotContain("secret");
    }

    [Fact]
    public void CreateConnectionException_WithInnerException_CreatesException()
    {
        // Arrange
        const string message = "Connection failed";
        var innerException = new InvalidOperationException("Inner error");

        // Act
        var exception = RedshiftExceptionFactory.CreateConnectionException(message, null, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.InnerException.Should().Be(innerException);
    }

    private sealed class TestEntity
    {
        public string? Name { get; set; }
    }
}
