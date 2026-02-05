using FluentAssertions;
using NPipeline.Connectors.Configuration;
using Xunit;

namespace NPipeline.Connectors.Tests.Configuration;

public class TestDatabaseConfiguration : DatabaseConfigurationBase
{
}

public class DatabaseConfigurationBaseTests
{
    [Fact]
    public void Constructor_SetsDefaultValues()
    {
        var config = new TestDatabaseConfiguration();

        config.ConnectionString.Should().BeEmpty();
        config.CommandTimeout.Should().Be(30);
        config.ConnectionTimeout.Should().Be(15);
        config.MinPoolSize.Should().Be(1);
        config.MaxPoolSize.Should().Be(100);
        config.ValidateIdentifiers.Should().BeTrue();
        config.DeliverySemantic.Should().Be(DeliverySemantic.AtLeastOnce);
        config.CheckpointStrategy.Should().Be(CheckpointStrategy.None);
    }

    [Fact]
    public void Validate_WithValidConfiguration_DoesNotThrow()
    {
        var config = new TestDatabaseConfiguration
        {
            ConnectionString = "Server=.;Database=Test;Trusted_Connection=True;",
            CommandTimeout = 10,
            ConnectionTimeout = 5,
            MinPoolSize = 1,
            MaxPoolSize = 10,
            CheckpointStrategy = CheckpointStrategy.None,
        };

        config.Invoking(c => c.Validate()).Should().NotThrow();
    }

    [Fact]
    public void Validate_WithInMemoryCheckpoint_IsAllowed()
    {
        var config = new TestDatabaseConfiguration
        {
            ConnectionString = "Server=.;Database=Test;Trusted_Connection=True;",
            CheckpointStrategy = CheckpointStrategy.InMemory,
        };

        config.Invoking(c => c.Validate()).Should().NotThrow();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Validate_WhenConnectionStringMissing_Throws(string? connectionString)
    {
        var config = new TestDatabaseConfiguration
        {
            ConnectionString = connectionString!,
        };

        config.Invoking(c => c.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("ConnectionString is required");
    }

    [Fact]
    public void Validate_WhenCommandTimeoutNegative_Throws()
    {
        var config = new TestDatabaseConfiguration
        {
            ConnectionString = "Server=.;Database=Test;Trusted_Connection=True;",
            CommandTimeout = -1,
        };

        config.Invoking(c => c.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("CommandTimeout must be non-negative");
    }

    [Fact]
    public void Validate_WhenConnectionTimeoutNegative_Throws()
    {
        var config = new TestDatabaseConfiguration
        {
            ConnectionString = "Server=.;Database=Test;Trusted_Connection=True;",
            ConnectionTimeout = -1,
        };

        config.Invoking(c => c.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("ConnectionTimeout must be non-negative");
    }

    [Fact]
    public void Validate_WhenMinPoolSizeNegative_Throws()
    {
        var config = new TestDatabaseConfiguration
        {
            ConnectionString = "Server=.;Database=Test;Trusted_Connection=True;",
            MinPoolSize = -1,
        };

        config.Invoking(c => c.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("MinPoolSize must be non-negative");
    }

    [Fact]
    public void Validate_WhenMaxPoolSizeLowerThanMinPoolSize_Throws()
    {
        var config = new TestDatabaseConfiguration
        {
            ConnectionString = "Server=.;Database=Test;Trusted_Connection=True;",
            MinPoolSize = 10,
            MaxPoolSize = 5,
        };

        config.Invoking(c => c.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("MaxPoolSize must be >= MinPoolSize");
    }

    [Fact]
    public void Validate_WhenUnsupportedCheckpointStrategy_Throws()
    {
        var config = new TestDatabaseConfiguration
        {
            ConnectionString = "Server=.;Database=Test;Trusted_Connection=True;",
            CheckpointStrategy = CheckpointStrategy.Offset,
        };

        config.Invoking(c => c.Validate())
            .Should().Throw<InvalidOperationException>()
            .WithMessage("Only None and InMemory checkpoint strategies are supported");
    }
}
