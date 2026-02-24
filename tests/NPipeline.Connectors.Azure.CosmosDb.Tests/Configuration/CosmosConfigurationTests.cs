using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Tests.Configuration;

public sealed class CosmosConfigurationTests
{
    [Fact]
    public void DefaultConfiguration_ShouldHaveValidDefaults()
    {
        // Arrange & Act
        var config = new CosmosConfiguration();

        // Assert
        config.ApiType.Should().Be(CosmosApiType.Sql);
        config.ConnectionString.Should().BeEmpty();
        config.AccountEndpoint.Should().BeEmpty();
        config.AccountKey.Should().BeEmpty();
        config.DatabaseId.Should().BeEmpty();
        config.ContainerId.Should().BeNull();
        config.MongoConnectionString.Should().BeNull();
        config.CassandraContactPoint.Should().BeNull();
        config.CassandraPort.Should().Be(10350);
        config.CassandraUsername.Should().BeNull();
        config.CassandraPassword.Should().BeNull();
        config.AuthenticationMode.Should().Be(CosmosAuthenticationMode.ConnectionString);
        config.NamedConnection.Should().BeNull();
        config.ConnectionTimeout.Should().Be(30);
        config.RequestTimeout.Should().Be(60);
        config.ConsistencyLevel.Should().BeNull();
        config.PreferredRegions.Should().BeEmpty();
        config.UseGatewayMode.Should().BeFalse();
        config.WriteStrategy.Should().Be(CosmosWriteStrategy.Upsert);
        config.UseUpsert.Should().BeTrue();
        config.BatchSize.Should().Be(100);
        config.MaxBatchSize.Should().Be(100);
        config.UseTransactionalBatch.Should().BeFalse();
        config.UseIfMatchEtag.Should().BeFalse();
        config.EnableContentResponseOnWrite.Should().BeFalse();
        config.Throughput.Should().BeNull();
        config.AutoCreateContainer.Should().BeFalse();
        config.AllowBulkExecution.Should().BeTrue();
        config.MaxConcurrentOperations.Should().Be(32);
        config.StreamResults.Should().BeTrue();
        config.MaxItemCount.Should().Be(1000);
        config.EnableCrossPartitionQuery.Should().BeTrue();
        config.ContinuationToken.Should().BeNull();
        config.ContinueOnError.Should().BeFalse();
        config.ThrowOnMappingError.Should().BeTrue();
        config.CacheMappingMetadata.Should().BeTrue();
        config.CaseInsensitiveMapping.Should().BeTrue();
        config.PartitionKeyPath.Should().BeNull();
        config.PartitionKeyHandling.Should().Be(PartitionKeyHandling.Auto);
        config.DeliverySemantic.Should().Be(DeliverySemantic.AtLeastOnce);
        config.CheckpointStrategy.Should().Be(CheckpointStrategy.None);
    }

    [Fact]
    public void Validate_WithValidSqlConnectionString_ShouldNotThrow()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            ConnectionString = "AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=testkey;",
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithValidAccountEndpointAndKey_ShouldNotThrow()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            AuthenticationMode = CosmosAuthenticationMode.AccountEndpointAndKey,
            AccountEndpoint = "https://account.documents.azure.com:443/",
            AccountKey = "testkey",
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithValidAzureAdCredential_ShouldNotThrow()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            AuthenticationMode = CosmosAuthenticationMode.AzureAdCredential,
            AccountEndpoint = "https://account.documents.azure.com:443/",
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithMissingConnectionString_ShouldThrowInvalidOperationException()
    {
        // Arrange - Set AccountEndpoint to trigger "hasSqlConnectionHints" but leave ConnectionString empty
        var config = new CosmosConfiguration
        {
            AuthenticationMode = CosmosAuthenticationMode.ConnectionString,
            AccountEndpoint = "https://account.documents.azure.com:443/", // This triggers the connection hints check
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("ConnectionString is required when AuthenticationMode is ConnectionString");
    }

    [Fact]
    public void Validate_WithMissingAccountEndpoint_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            AuthenticationMode = CosmosAuthenticationMode.AccountEndpointAndKey,
            AccountKey = "testkey",
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("AccountEndpoint is required when AuthenticationMode is AccountEndpointAndKey");
    }

    [Fact]
    public void Validate_WithMissingAccountKey_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            AuthenticationMode = CosmosAuthenticationMode.AccountEndpointAndKey,
            AccountEndpoint = "https://account.documents.azure.com:443/",
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("AccountKey is required when AuthenticationMode is AccountEndpointAndKey");
    }

    [Fact]
    public void Validate_WithMissingAccountEndpointForAzureAd_ShouldThrowInvalidOperationException()
    {
        // Arrange - Set AccountKey to trigger "hasSqlConnectionHints" but leave AccountEndpoint empty
        var config = new CosmosConfiguration
        {
            AuthenticationMode = CosmosAuthenticationMode.AzureAdCredential,
            AccountKey = "somekey", // This triggers the connection hints check
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("AccountEndpoint is required when AuthenticationMode is AzureAdCredential");
    }

    [Fact]
    public void Validate_WithMissingDatabaseId_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            ConnectionString = "AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=testkey;",
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("DatabaseId is required");
    }

    [Fact]
    public void Validate_WithZeroBatchSize_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            ConnectionString = "AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=testkey;",
            DatabaseId = "TestDatabase",
            BatchSize = 0,
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("BatchSize must be between 1 and");
    }

    [Fact]
    public void Validate_WithNegativeBatchSize_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            ConnectionString = "AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=testkey;",
            DatabaseId = "TestDatabase",
            BatchSize = -1,
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("BatchSize must be between 1 and");
    }

    [Fact]
    public void Validate_WithBatchSizeExceedingMaxBatchSize_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            ConnectionString = "AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=testkey;",
            DatabaseId = "TestDatabase",
            BatchSize = 150,
            MaxBatchSize = 100,
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("BatchSize must be between 1 and");
    }

    [Fact]
    public void Validate_WithZeroMaxConcurrentOperations_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            ConnectionString = "AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=testkey;",
            DatabaseId = "TestDatabase",
            MaxConcurrentOperations = 0,
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("MaxConcurrentOperations must be greater than 0");
    }

    [Fact]
    public void Validate_WithNegativeMaxConcurrentOperations_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            ConnectionString = "AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=testkey;",
            DatabaseId = "TestDatabase",
            MaxConcurrentOperations = -1,
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("MaxConcurrentOperations must be greater than 0");
    }

    [Fact]
    public void Validate_WithValidMongoApi_ShouldNotThrow()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            ApiType = CosmosApiType.Mongo,
            MongoConnectionString = "mongodb://account:password@account.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false",
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithMissingMongoConnectionString_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            ApiType = CosmosApiType.Mongo,
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("MongoConnectionString or ConnectionString is required when ApiType is Mongo");
    }

    [Fact]
    public void Validate_WithValidCassandraApi_ShouldNotThrow()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            ApiType = CosmosApiType.Cassandra,
            CassandraContactPoint = "account.cassandra.cosmos.azure.com",
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Validate_WithMissingCassandraContactPoint_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            ApiType = CosmosApiType.Cassandra,
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("CassandraContactPoint or AccountEndpoint is required when ApiType is Cassandra");
    }

    [Fact]
    public void Validate_WithZeroCassandraPort_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            ApiType = CosmosApiType.Cassandra,
            CassandraContactPoint = "account.cassandra.cosmos.azure.com",
            CassandraPort = 0,
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => config.Validate());
        exception.Message.Should().Contain("CassandraPort must be greater than 0");
    }

    [Fact]
    public void Validate_WithNamedConnection_ShouldNotRequireConnectionString()
    {
        // Arrange
        var config = new CosmosConfiguration
        {
            NamedConnection = "MyNamedConnection",
            DatabaseId = "TestDatabase",
        };

        // Act & Assert
        var exception = Record.Exception(() => config.Validate());
        exception.Should().BeNull();
    }

    [Fact]
    public void Clone_ShouldCreateDeepCopy()
    {
        // Arrange
        var original = new CosmosConfiguration
        {
            ConnectionString = "AccountEndpoint=https://account.documents.azure.com:443/;AccountKey=testkey;",
            ApiType = CosmosApiType.Sql,
            AccountEndpoint = "https://account.documents.azure.com:443/",
            AccountKey = "testkey",
            DatabaseId = "TestDatabase",
            ContainerId = "TestContainer",
            AuthenticationMode = CosmosAuthenticationMode.ConnectionString,
            ConnectionTimeout = 60,
            RequestTimeout = 120,
            ConsistencyLevel = ConsistencyLevel.Strong,
            PreferredRegions = ["East US", "West US"],
            UseGatewayMode = true,
            WriteStrategy = CosmosWriteStrategy.Bulk,
            BatchSize = 50,
            MaxBatchSize = 200,
            UseTransactionalBatch = true,
            UseIfMatchEtag = true,
            EnableContentResponseOnWrite = true,
            Throughput = 400,
            AutoCreateContainer = true,
            AllowBulkExecution = false,
            MaxConcurrentOperations = 64,
            StreamResults = false,
            MaxItemCount = 500,
            EnableCrossPartitionQuery = false,
            ContinuationToken = "token123",
            ContinueOnError = true,
            PartitionKeyPath = "/customerId",
            PartitionKeyHandling = PartitionKeyHandling.Explicit,
            DeliverySemantic = DeliverySemantic.AtMostOnce,
            CheckpointStrategy = CheckpointStrategy.InMemory,
        };

        // Act
        var clone = original.Clone();

        // Assert
        clone.Should().NotBeSameAs(original);
        clone.ConnectionString.Should().Be(original.ConnectionString);
        clone.ApiType.Should().Be(original.ApiType);
        clone.AccountEndpoint.Should().Be(original.AccountEndpoint);
        clone.AccountKey.Should().Be(original.AccountKey);
        clone.DatabaseId.Should().Be(original.DatabaseId);
        clone.ContainerId.Should().Be(original.ContainerId);
        clone.AuthenticationMode.Should().Be(original.AuthenticationMode);
        clone.ConnectionTimeout.Should().Be(original.ConnectionTimeout);
        clone.RequestTimeout.Should().Be(original.RequestTimeout);
        clone.ConsistencyLevel.Should().Be(original.ConsistencyLevel);
        clone.PreferredRegions.Should().NotBeSameAs(original.PreferredRegions);
        clone.PreferredRegions.Should().BeEquivalentTo(original.PreferredRegions);
        clone.UseGatewayMode.Should().Be(original.UseGatewayMode);
        clone.WriteStrategy.Should().Be(original.WriteStrategy);
        clone.BatchSize.Should().Be(original.BatchSize);
        clone.MaxBatchSize.Should().Be(original.MaxBatchSize);
        clone.UseTransactionalBatch.Should().Be(original.UseTransactionalBatch);
        clone.UseIfMatchEtag.Should().Be(original.UseIfMatchEtag);
        clone.EnableContentResponseOnWrite.Should().Be(original.EnableContentResponseOnWrite);
        clone.Throughput.Should().Be(original.Throughput);
        clone.AutoCreateContainer.Should().Be(original.AutoCreateContainer);
        clone.AllowBulkExecution.Should().Be(original.AllowBulkExecution);
        clone.MaxConcurrentOperations.Should().Be(original.MaxConcurrentOperations);
        clone.StreamResults.Should().Be(original.StreamResults);
        clone.MaxItemCount.Should().Be(original.MaxItemCount);
        clone.EnableCrossPartitionQuery.Should().Be(original.EnableCrossPartitionQuery);
        clone.ContinuationToken.Should().Be(original.ContinuationToken);
        clone.ContinueOnError.Should().Be(original.ContinueOnError);
        clone.PartitionKeyPath.Should().Be(original.PartitionKeyPath);
        clone.PartitionKeyHandling.Should().Be(original.PartitionKeyHandling);
        clone.DeliverySemantic.Should().Be(original.DeliverySemantic);
        clone.CheckpointStrategy.Should().Be(original.CheckpointStrategy);
    }

    [Fact]
    public void Clone_WhenModifyingClone_ShouldNotAffectOriginal()
    {
        // Arrange
        var original = new CosmosConfiguration
        {
            ConnectionString = "Original",
            DatabaseId = "OriginalDb",
            PreferredRegions = ["East US"],
        };

        // Act
        var clone = original.Clone();
        clone.ConnectionString = "Modified";
        clone.DatabaseId = "ModifiedDb";
        clone.PreferredRegions.Add("West US");

        // Assert
        original.ConnectionString.Should().Be("Original");
        original.DatabaseId.Should().Be("OriginalDb");
        original.PreferredRegions.Should().ContainSingle().Which.Should().Be("East US");
    }

    [Fact]
    public void WriteBatchSize_ShouldBeAliasForBatchSize()
    {
        // Arrange
        var config = new CosmosConfiguration();

        // Act
        config.WriteBatchSize = 200;

        // Assert
        config.BatchSize.Should().Be(200);
        config.WriteBatchSize.Should().Be(200);
    }

    [Fact]
    public void CommandTimeout_ShouldBeAliasForRequestTimeout()
    {
        // Arrange
        var config = new CosmosConfiguration();

        // Act
        config.CommandTimeout = 120;

        // Assert
        config.RequestTimeout.Should().Be(120);
        config.CommandTimeout.Should().Be(120);
    }

    [Fact]
    public void MaxConcurrency_ShouldBeAliasForMaxConcurrentOperations()
    {
        // Arrange
        var config = new CosmosConfiguration();

        // Act
        config.MaxConcurrency = 64;

        // Assert
        config.MaxConcurrentOperations.Should().Be(64);
        config.MaxConcurrency.Should().Be(64);
    }

    [Fact]
    public void FetchSize_ShouldBeAliasForMaxItemCount()
    {
        // Arrange
        var config = new CosmosConfiguration();

        // Act
        config.FetchSize = 500;

        // Assert
        config.MaxItemCount.Should().Be(500);
        config.FetchSize.Should().Be(500);
    }

    [Fact]
    public void PreferredRegions_ShouldBeMutable()
    {
        // Arrange
        var config = new CosmosConfiguration();

        // Act
        config.PreferredRegions.Add("East US");
        config.PreferredRegions.Add("West US");

        // Assert
        config.PreferredRegions.Should().HaveCount(2);
        config.PreferredRegions.Should().Contain(["East US", "West US"]);
    }
}
