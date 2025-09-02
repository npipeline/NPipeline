using AwesomeAssertions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.TypeConversion;

#pragma warning disable CA1711,IDE0005,IDE0161,CA1309

public sealed class TypeConversionTests
{
    [Fact]
    public async Task TypeConversionNode_DefaultFactory_CreatesEmptyDestination()
    {
        // Arrange
        TypeConversionNode<Source, Destination> node = new();
        var ctx = PipelineContext.Default;
        Source input = new() { Name = "test", Id = 42, Amount = 99.99m };

        // Act
        var result = await node.ExecuteAsync(input, ctx, CancellationToken.None);

        // Assert
        _ = result.Should().NotBeNull();
        _ = result.Name.Should().BeEmpty();
        _ = result.Id.Should().Be(0);
        _ = result.Amount.Should().Be(0m);
    }

    [Fact]
    public async Task TypeConversionNode_CustomFactory_UsesFactory()
    {
        // Arrange
        Destination MakeDestination(Source s)
        {
            return new Destination { Name = s.Name, Id = s.Id * 2 };
        }

        TypeConversionNode<Source, Destination> node = new(MakeDestination);
        var ctx = PipelineContext.Default;
        Source input = new() { Name = "hello", Id = 5, Amount = 10m };

        // Act
        var result = await node.ExecuteAsync(input, ctx, CancellationToken.None);

        // Assert
        _ = result.Name.Should().Be("hello");
        _ = result.Id.Should().Be(10);
    }

    [Fact]
    public async Task TypeConversionNode_AutoMap_MatchesByName()
    {
        // Arrange
        Source input = new() { Name = "Product", Id = 100, Amount = 50.5m };
        TypeConversionNode<Source, Destination> node = new();
        _ = node.AutoMap();
        var ctx = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(input, ctx, CancellationToken.None);

        // Assert
        _ = result.Name.Should().Be("Product");
        _ = result.Id.Should().Be(100);
        _ = result.Amount.Should().Be(50.5m);
    }

    [Fact]
    public async Task TypeConversionNode_Map_AppliesConversionRule()
    {
        // Arrange
        Source input = new() { Name = "test", Id = 5, Amount = 20m };
        TypeConversionNode<Source, Destination> node = new();
        _ = node.Map(s => s.Id, d => d.Id, id => id * 3);
        var ctx = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(input, ctx, CancellationToken.None);

        // Assert
        _ = result.Id.Should().Be(15);
    }

    [Fact]
    public async Task TypeConversionNode_MapWithWholeInput_UsesFullSource()
    {
        // Arrange
        Source input = new() { Name = "item", Id = 7, Amount = 100m };
        TypeConversionNode<Source, Destination> node = new();
        _ = node.Map(d => d.Name, s => $"ID:{s.Id}");
        var ctx = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(input, ctx, CancellationToken.None);

        // Assert
        _ = result.Name.Should().Be("ID:7");
    }

    [Fact]
    public async Task TypeConversionNode_MultipleRules_AllApply()
    {
        // Arrange
        Source input = new() { Name = "orig", Id = 10, Amount = 50m };
        TypeConversionNode<Source, Destination> node = new();

        _ = node
            .Map(s => s.Id, d => d.Id, i => i + 1)
            .Map(s => s.Amount, d => d.Amount, a => a * 2);

        var ctx = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(input, ctx, CancellationToken.None);

        // Assert
        _ = result.Id.Should().Be(11);
        _ = result.Amount.Should().Be(100m);
    }

    [Fact]
    public async Task TypeConversionNode_AutoMapThenManualOverride_ManualWins()
    {
        // Arrange
        Source input = new() { Name = "default", Id = 5, Amount = 25m };
        TypeConversionNode<Source, Destination> node = new();
        _ = node.AutoMap();
        _ = node.Map(s => s.Name, d => d.Name, n => $"Overridden:{n}");
        var ctx = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(input, ctx, CancellationToken.None);

        // Assert
        _ = result.Name.Should().Be("Overridden:default");
        _ = result.Id.Should().Be(5);
    }

    [Fact]
    public void TypeConverterFactory_StringToInt_Works()
    {
        // Arrange
        var factory = TypeConverterFactory.CreateDefault();

        // Act
        var ok = factory.TryGetConverter<string, int>(out var converter);

        // Assert
        _ = ok.Should().BeTrue();
        _ = converter!("42").Should().Be(42);
    }

    [Fact]
    public void TypeConverterFactory_StringToDouble_Works()
    {
        // Arrange
        var factory = TypeConverterFactory.CreateDefault();

        // Act
        var ok = factory.TryGetConverter<string, double>(out var converter);

        // Assert
        _ = ok.Should().BeTrue();
        _ = converter!("3.14").Should().BeApproximately(3.14, 0.001);
    }

    [Fact]
    public void TypeConverterFactory_StringToDecimal_Works()
    {
        // Arrange
        var factory = TypeConverterFactory.CreateDefault();

        // Act
        var ok = factory.TryGetConverter<string, decimal>(out var converter);

        // Assert
        _ = ok.Should().BeTrue();
        _ = converter!("99.99").Should().Be(99.99m);
    }

    [Fact]
    public void TypeConverterFactory_StringToBool_Works()
    {
        // Arrange
        var factory = TypeConverterFactory.CreateDefault();

        // Act
        var ok = factory.TryGetConverter<string, bool>(out var converter);

        // Assert
        _ = ok.Should().BeTrue();
        _ = converter!("true").Should().BeTrue();
        _ = converter!("false").Should().BeFalse();
    }

    [Fact]
    public void TypeConverterFactory_StringToGuid_Works()
    {
        // Arrange
        var expected = Guid.NewGuid();
        var factory = TypeConverterFactory.CreateDefault();

        // Act
        var ok = factory.TryGetConverter<string, Guid>(out var converter);

        // Assert
        _ = ok.Should().BeTrue();
        _ = converter!(expected.ToString()).Should().Be(expected);
    }

    [Fact]
    public void TypeConverterFactory_StringToDateTime_Works()
    {
        // Arrange
        var factory = TypeConverterFactory.CreateDefault();

        // Act
        var ok = factory.TryGetConverter<string, DateTime>(out var converter);

        // Assert
        _ = ok.Should().BeTrue();
        var result = converter!("2023-01-15T10:30:00Z");
        _ = result.Year.Should().Be(2023);
        _ = result.Month.Should().Be(1);
        _ = result.Day.Should().Be(15);
    }

    [Fact]
    public void TypeConverterFactory_StringToTimeSpan_Works()
    {
        // Arrange
        var factory = TypeConverterFactory.CreateDefault();

        // Act
        var ok = factory.TryGetConverter<string, TimeSpan>(out var converter);

        // Assert
        _ = ok.Should().BeTrue();
        var result = converter!("01:30:45");
        _ = result.Hours.Should().Be(1);
        _ = result.Minutes.Should().Be(30);
        _ = result.Seconds.Should().Be(45);
    }

    [Fact]
    public void TypeConverterFactory_StringToString_Returns()
    {
        // Arrange
        var factory = TypeConverterFactory.CreateDefault();

        // Act
        var ok = factory.TryGetConverter<string, string>(out var converter);

        // Assert
        _ = ok.Should().BeTrue();
        _ = converter!("hello").Should().Be("hello");
    }

    [Fact]
    public void TypeConverterFactory_IdentityTypes_Works()
    {
        // Arrange
        var factory = TypeConverterFactory.CreateDefault();

        // Act
        var ok = factory.TryGetConverter(out Func<int, int>? converter);

        // Assert
        _ = ok.Should().BeTrue();
        _ = converter!(42).Should().Be(42);
    }

    [Fact]
    public void TypeConverterFactory_RegisterCustom_Works()
    {
        // Arrange
        TypeConverterFactory factory = new();
        factory.Register<int, string>(i => $"Value:{i}");

        // Act
        var ok = factory.TryGetConverter<int, string>(out var converter);

        // Assert
        _ = ok.Should().BeTrue();
        _ = converter!(42).Should().Be("Value:42");
    }

    [Fact]
    public void TypeConverterFactory_StringToNullableInt_Works()
    {
        // Arrange
        var factory = TypeConverterFactory.CreateDefault();

        // Act
        var ok = factory.TryGetConverter<string, int?>(out var converter);

        // Assert
        _ = ok.Should().BeTrue();
        _ = converter!("100").Should().Be(100);
        _ = converter!(string.Empty).Should().BeNull();
    }

    [Fact]
    public void TypeConverterFactory_UnknownConversion_ReturnsFalse()
    {
        // Arrange
        var factory = TypeConverterFactory.CreateDefault();

        // Act
        var ok = factory.TryGetConverter(typeof(byte[]), typeof(char[]), out _);

        // Assert
        _ = ok.Should().BeFalse();
    }

    [Fact]
    public void TypeConversionNode_MapNullSource_Throws()
    {
        // Arrange
        TypeConversionNode<Source, Destination> node = new();

        // Act & Assert
        _ = node.Invoking(n => n.Map(
            null!,
            d => d.Name,
            (string s) => s
        )).Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void TypeConversionNode_MapNullDestination_Throws()
    {
        // Arrange
        TypeConversionNode<Source, Destination> node = new();

        // Act & Assert
        _ = node.Invoking(n => n.Map(
            s => s.Name,
            null!,
            s => s
        )).Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void TypeConversionNode_MapNullConverter_Throws()
    {
        // Arrange
        TypeConversionNode<Source, Destination> node = new();

        // Act & Assert
        _ = node.Invoking(n => n.Map(
            s => s.Name,
            d => d.Name,
            (Func<string, string>)null!
        )).Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void TypeConverterFactory_NullSourceType_Throws()
    {
        // Arrange
        var factory = TypeConverterFactory.CreateDefault();

        // Act & Assert
        _ = factory.Invoking(f => f.TryGetConverter(null!, typeof(int), out _))
            .Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void TypeConverterFactory_NullDestinationType_Throws()
    {
        // Arrange
        var factory = TypeConverterFactory.CreateDefault();

        // Act & Assert
        _ = factory.Invoking(f => f.TryGetConverter(typeof(string), null!, out _))
            .Should().Throw<ArgumentNullException>();
    }

    private sealed class Source
    {
        public string Name { get; set; } = string.Empty;
        public int Id { get; set; }
        public decimal Amount { get; set; }
    }

    private sealed class Destination
    {
        public string Name { get; set; } = string.Empty;
        public int Id { get; set; }
        public decimal Amount { get; set; }
    }
}
