using AwesomeAssertions;
using NPipeline.Extensions.Nodes.Core;

namespace NPipeline.Extensions.Nodes.Tests.Core;

public sealed class PropertyAccessorTests
{
    [Fact]
    public void Create_WithSimpleProperty_ShouldCompileGetterAndSetter()
    {
        // Arrange & Act
        var accessor = PropertyAccessor.Create<TestData, string>(x => x.Name);

        // Assert
        accessor.Should().NotBeNull();
        accessor.Getter.Should().NotBeNull();
        accessor.Setter.Should().NotBeNull();
    }

    [Fact]
    public void Accessor_WithSimpleProperty_ShouldGetAndSetValue()
    {
        // Arrange
        var accessor = PropertyAccessor.Create<TestData, string>(x => x.Name);
        var data = new TestData { Name = "Alice" };

        // Act
        var value = accessor.Getter(data);
        accessor.Setter(data, "Bob");
        var newValue = accessor.Getter(data);

        // Assert
        value.Should().Be("Alice");
        newValue.Should().Be("Bob");
        data.Name.Should().Be("Bob");
    }

    [Fact]
    public void Accessor_WithNestedProperty_ShouldGetAndSetValue()
    {
        // Arrange
        var accessor = PropertyAccessor.Create<TestData, string>(x => x.Address!.City);
        var data = new TestData { Address = new Address { City = "NYC" } };

        // Act
        var value = accessor.Getter(data);
        accessor.Setter(data, "LA");
        var newValue = accessor.Getter(data);

        // Assert
        value.Should().Be("NYC");
        newValue.Should().Be("LA");
        data.Address.City.Should().Be("LA");
    }

    [Fact]
    public void Accessor_WithValueTypeProperty_ShouldGetAndSetValue()
    {
        // Arrange
        var accessor = PropertyAccessor.Create<TestData, int>(x => x.Age);
        var data = new TestData { Age = 30 };

        // Act
        var value = accessor.Getter(data);
        accessor.Setter(data, 40);
        var newValue = accessor.Getter(data);

        // Assert
        value.Should().Be(30);
        newValue.Should().Be(40);
        data.Age.Should().Be(40);
    }

    [Fact]
    public void Create_WithReadOnlyProperty_ShouldThrowArgumentException()
    {
        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() =>
            PropertyAccessor.Create<TestData, string>(x => x.ReadOnlyProperty));

        ex.Message.Should().Contain("does not have a public setter");
    }

    [Fact]
    public void Create_WithNullSelector_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            PropertyAccessor.Create<TestData, string>(null!));
    }

    [Fact]
    public void Accessor_WithMultipleInstances_ShouldWorkIndependently()
    {
        // Arrange
        var accessor = PropertyAccessor.Create<TestData, string>(x => x.Name);
        var data1 = new TestData { Name = "Alice" };
        var data2 = new TestData { Name = "Bob" };

        // Act
        accessor.Setter(data1, "Charlie");
        accessor.Setter(data2, "David");

        // Assert
        data1.Name.Should().Be("Charlie");
        data2.Name.Should().Be("David");
    }

    private sealed class TestData
    {
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public Address? Address { get; set; }
        public string ReadOnlyProperty => "readonly";
    }

    private sealed class Address
    {
        public string City { get; set; } = string.Empty;
        public string Street { get; set; } = string.Empty;
    }
}
