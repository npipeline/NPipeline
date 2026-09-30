using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Messaging;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Configuration;

public class ServiceBusOptionsTests
{
    internal const string ConnectionString = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=abc=";
    internal const string Namespace = "test.servicebus.windows.net";

    private static ServiceBusReadOptions Read() => new() { ConnectionString = ConnectionString, Entity = "orders" };

    private static ServiceBusWriteOptions<int> Write() => new() { ConnectionString = ConnectionString, Entity = "orders" };

    public class Connection
    {
        [Fact]
        public void Validate_WithNoConnection_Throws()
        {
            var options = new ServiceBusReadOptions { Entity = "orders" };

            options.Invoking(o => o.Validate()).Should().ThrowExactly<ArgumentException>().WithMessage("*exactly one*");
        }

        [Fact]
        public void Validate_WithWhitespaceConnectionString_Throws()
        {
            var options = Read() with { ConnectionString = "  " };

            options.Invoking(o => o.Validate()).Should().ThrowExactly<ArgumentException>();
        }

        [Fact]
        public async Task Validate_WithClientAndConnectionString_Throws()
        {
            await using var client = new ServiceBusClient(ConnectionString);
            var options = Read() with { Client = client };

            options.Invoking(o => o.Validate()).Should().ThrowExactly<ArgumentException>().WithMessage("*exactly one*");
        }

        [Fact]
        public void Validate_WithConnectionStringAndNamespace_Throws()
        {
            var options = Write() with { FullyQualifiedNamespace = Namespace };

            options.Invoking(o => o.Validate()).Should().ThrowExactly<ArgumentException>();
        }

        [Fact]
        public async Task Validate_WithOnlyClient_Succeeds()
        {
            await using var client = new ServiceBusClient(ConnectionString);
            var options = Read() with { ConnectionString = null, Client = client };

            options.Invoking(o => o.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WithOnlyNamespace_Succeeds()
        {
            var options = Write() with { ConnectionString = null, FullyQualifiedNamespace = Namespace };

            options.Invoking(o => o.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WithNullSerializer_Throws()
        {
            var options = Read() with { Serializer = null! };

            options.Invoking(o => o.Validate()).Should().Throw<ArgumentNullException>();
        }
    }

    public class ReadOptions
    {
        [Fact]
        public void Defaults_AreAsDocumented()
        {
            var options = Read();

            options.Subscription.Should().BeNull();
            options.SubQueue.Should().Be(SubQueue.None);
            options.MaxInFlight.Should().Be(100);
            options.PrefetchCount.Should().Be(0);
            options.MaxLockRenewal.Should().Be(TimeSpan.FromMinutes(5));
            options.RowErrorHandler.Should().BeNull();
            options.RawExcerptLength.Should().Be(256);
            options.MaxConcurrentSessions.Should().Be(8);
            options.SessionIdleTimeout.Should().Be(TimeSpan.FromSeconds(5));
            options.SettleTimeout.Should().Be(TimeSpan.FromSeconds(30));
            options.Serializer.Should().BeSameAs(JsonMessageSerializer.Default);
            options.Retry.Should().BeNull();
            options.Credential.Should().BeNull();
        }

        [Fact]
        public void Validate_WithValidOptions_Succeeds()
        {
            Read().Invoking(o => o.Validate()).Should().NotThrow();
        }

        [Theory]
        [InlineData("")]
        [InlineData("   ")]
        public void Validate_WithBlankEntity_Throws(string entity)
        {
            var options = Read() with { Entity = entity };

            options.Invoking(o => o.Validate()).Should().Throw<ArgumentException>();
        }

        [Fact]
        public void Validate_WithZeroMaxInFlight_Throws()
        {
            (Read() with { MaxInFlight = 0 }).Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public void Validate_WithNegativePrefetchCount_Throws()
        {
            (Read() with { PrefetchCount = -1 }).Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public void Validate_WithNegativeMaxLockRenewal_Throws()
        {
            (Read() with { MaxLockRenewal = TimeSpan.FromSeconds(-1) }).Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public void Validate_WithZeroMaxLockRenewal_Succeeds()
        {
            (Read() with { MaxLockRenewal = TimeSpan.Zero }).Invoking(o => o.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WithNegativeRawExcerptLength_Throws()
        {
            (Read() with { RawExcerptLength = -1 }).Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public void Validate_WithZeroMaxConcurrentSessions_Throws()
        {
            (Read() with { MaxConcurrentSessions = 0 }).Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public void Validate_WithZeroSessionIdleTimeout_Throws()
        {
            (Read() with { SessionIdleTimeout = TimeSpan.Zero }).Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public void Validate_WithNegativeSettleTimeout_Throws()
        {
            (Read() with { SettleTimeout = TimeSpan.FromSeconds(-1) }).Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public void Validate_WithZeroSettleTimeout_Succeeds()
        {
            (Read() with { SettleTimeout = TimeSpan.Zero }).Invoking(o => o.Validate()).Should().NotThrow();
        }
    }

    public class WriteOptions
    {
        [Fact]
        public void Defaults_AreAsDocumented()
        {
            var options = Write();

            options.BatchSize.Should().Be(100);
            options.BatchLinger.Should().Be(TimeSpan.FromMilliseconds(10));
            options.CopyMessageProperties.Should().BeTrue();
            options.MessageId.Should().BeNull();
            options.SessionId.Should().BeNull();
            options.Subject.Should().BeNull();
            options.TimeToLive.Should().BeNull();
            options.FailedMessages.Should().Be(FailedMessageAction.Fail);
            options.Serializer.Should().BeSameAs(JsonMessageSerializer.Default);
        }

        [Fact]
        public void Validate_WithValidOptions_Succeeds()
        {
            Write().Invoking(o => o.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WithBlankEntity_Throws()
        {
            (Write() with { Entity = " " }).Invoking(o => o.Validate()).Should().Throw<ArgumentException>();
        }

        [Fact]
        public void Validate_WithNoConnection_Throws()
        {
            new ServiceBusWriteOptions<int> { Entity = "orders" }.Invoking(o => o.Validate()).Should().ThrowExactly<ArgumentException>();
        }

        [Fact]
        public void Validate_WithZeroBatchSize_Throws()
        {
            (Write() with { BatchSize = 0 }).Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public void Validate_WithNegativeBatchLinger_Throws()
        {
            (Write() with { BatchLinger = TimeSpan.FromMilliseconds(-5) }).Invoking(o => o.Validate()).Should().Throw<ArgumentOutOfRangeException>();
        }

        [Fact]
        public void Validate_WithInfiniteBatchLinger_Succeeds()
        {
            (Write() with { BatchLinger = Timeout.InfiniteTimeSpan }).Invoking(o => o.Validate()).Should().NotThrow();
        }

        [Fact]
        public void Validate_WithZeroBatchLinger_Succeeds()
        {
            (Write() with { BatchLinger = TimeSpan.Zero }).Invoking(o => o.Validate()).Should().NotThrow();
        }
    }
}
