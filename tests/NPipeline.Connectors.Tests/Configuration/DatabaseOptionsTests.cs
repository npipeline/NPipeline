using FluentAssertions;
using NPipeline.Connectors.Configuration;
using Xunit;

namespace NPipeline.Connectors.Tests.Configuration;

public class DatabaseOptionsTests
{
    [Fact]
    public void Constructor_SetsDefaultValues()
    {
        var options = new DatabaseOptions();

        options.DefaultConnectionString.Should().BeEmpty();
        options.NamedConnections.Should().BeEmpty();
    }

    [Fact]
    public void DefaultConnectionString_CanBeSet()
    {
        var options = new DatabaseOptions
        {
            DefaultConnectionString = "Default Connection",
        };

        options.DefaultConnectionString.Should().Be("Default Connection");
    }

    [Fact]
    public void NamedConnections_IsCaseInsensitive()
    {
        var options = new DatabaseOptions();

        options.NamedConnections["Primary"] = "Connection1";
        options.NamedConnections["primary"] = "Connection2";

        options.NamedConnections.Should().HaveCount(1);
        options.NamedConnections["primary"].Should().Be("Connection2");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void GetConnectionString_WithNullOrWhitespaceName_ReturnsDefault(string? name)
    {
        var options = new DatabaseOptions
        {
            DefaultConnectionString = "Default Connection",
        };

        options.GetConnectionString(name).Should().Be("Default Connection");
    }

    [Fact]
    public void GetConnectionString_WithValidName_ReturnsNamedConnection()
    {
        var options = new DatabaseOptions
        {
            DefaultConnectionString = "Default Connection",
        };

        options.NamedConnections["Primary"] = "Primary Connection";

        options.GetConnectionString("Primary").Should().Be("Primary Connection");
    }

    [Fact]
    public void GetConnectionString_WithMissingName_Throws()
    {
        var options = new DatabaseOptions
        {
            DefaultConnectionString = "Default Connection",
        };

        var act = () => options.GetConnectionString("Missing");

        act.Should().Throw<InvalidOperationException>()
            .WithMessage("Named connection 'Missing' not found");
    }

    [Fact]
    public void NamedConnections_CanAddConnection()
    {
        var options = new DatabaseOptions();

        options.NamedConnections["Primary"] = "Primary Connection";

        options.NamedConnections.Should().HaveCount(1);
        options.NamedConnections["Primary"].Should().Be("Primary Connection");
    }

    [Fact]
    public void NamedConnections_CanRemoveConnection()
    {
        var options = new DatabaseOptions();
        options.NamedConnections["Primary"] = "Primary Connection";

        options.NamedConnections.Remove("Primary");

        options.NamedConnections.Should().BeEmpty();
    }

    [Fact]
    public void NamedConnections_ClearRemovesAllConnections()
    {
        var options = new DatabaseOptions();
        options.NamedConnections["Primary"] = "Primary Connection";
        options.NamedConnections["Secondary"] = "Secondary Connection";

        options.NamedConnections.Clear();

        options.NamedConnections.Should().BeEmpty();
    }
}
