using AwesomeAssertions;
using NPipeline.Extensions.Nodes.Core.Exceptions;

namespace NPipeline.Extensions.Nodes.Tests.Core.Exceptions;

public sealed class NodeExceptionsTests
{
    [Fact]
    public void ValidationException_Constructor_ShouldSetProperties()
    {
        // Arrange & Act
        var ex = new ValidationException("Name", "NotEmpty", "Bob", "Name cannot be empty");

        // Assert
        ex.PropertyPath.Should().Be("Name");
        ex.RuleName.Should().Be("NotEmpty");
        ex.Value.Should().Be("Bob");
        ex.Message.Should().Contain("Name cannot be empty");
    }

    [Fact]
    public void ValidationException_WithNullMessage_ShouldGenerateDefaultMessage()
    {
        // Arrange & Act
        var ex = new ValidationException("Age", "Range", 150, null!);

        // Assert
        // When message is null, default message is generated
        ex.Message.Should().NotBeNullOrEmpty();
        ex.PropertyPath.Should().Be("Age");
        ex.RuleName.Should().Be("Range");
        ex.Value.Should().Be(150);
    }

    [Fact]
    public void ValidationException_Message_ShouldContainAllDetails()
    {
        // Arrange & Act
        var ex = new ValidationException("Email", "ValidFormat", "invalid", "Invalid email format");

        // Assert
        ex.PropertyPath.Should().Be("Email");
        ex.RuleName.Should().Be("ValidFormat");
        ex.Value.Should().Be("invalid");
        ex.Message.Should().Be("Invalid email format");
    }

    [Fact]
    public void FilteringException_Constructor_ShouldSetReason()
    {
        // Arrange & Act
        var ex = new FilteringException("Item does not meet criteria");

        // Assert
        ex.Reason.Should().Be("Item does not meet criteria");
        ex.Message.Should().Contain("Item does not meet criteria");
    }

    [Fact]
    public void FilteringException_WithNullReason_ShouldGenerateDefaultMessage()
    {
        // Arrange & Act
        var ex = new FilteringException(null!);

        // Assert
        ex.Message.Should().NotBeNullOrWhiteSpace();
    }

    [Fact]
    public void TypeConversionException_Constructor_ShouldSetProperties()
    {
        // Arrange & Act
        var ex = new TypeConversionException(
            typeof(string),
            typeof(int),
            "abc",
            "Cannot convert string to int");

        // Assert
        ex.SourceType.Should().BeSameAs(typeof(string));
        ex.TargetType.Should().BeSameAs(typeof(int));
        ex.Value.Should().Be("abc");
        ex.Message.Should().Contain("Cannot convert string to int");
    }

    [Fact]
    public void TypeConversionException_WithNullMessage_ShouldGenerateDefaultMessage()
    {
        // Arrange & Act
        var ex = new TypeConversionException(typeof(string), typeof(double), "xyz", null!);

        // Assert
        ex.SourceType.Should().BeSameAs(typeof(string));
        ex.TargetType.Should().BeSameAs(typeof(double));
        ex.Value.Should().Be("xyz");
        ex.Message.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public void TypeConversionException_Message_ShouldUseProvidedMessage()
    {
        // Arrange & Act
        var ex = new TypeConversionException(
            typeof(DateTime),
            typeof(DateOnly),
            DateTime.Now,
            "Invalid date conversion");

        // Assert
        ex.SourceType.Should().BeSameAs(typeof(DateTime));
        ex.TargetType.Should().BeSameAs(typeof(DateOnly));
        ex.Message.Should().Be("Invalid date conversion");
    }
}
