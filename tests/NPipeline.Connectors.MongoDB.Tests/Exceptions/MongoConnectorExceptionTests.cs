using MongoDB.Bson;
using NPipeline.Connectors.MongoDB.Exceptions;

namespace NPipeline.Connectors.MongoDB.Tests.Exceptions;

public class MongoConnectorExceptionTests
{
    [Fact]
    public void MongoConnectorException_MessageOnly_StoresMessage()
    {
        var ex = new MongoConnectorException("test error");
        ex.Message.Should().Be("test error");
        ex.CollectionName.Should().BeNull();
        ex.OperationContext.Should().BeNull();
        ex.InnerException.Should().BeNull();
    }

    [Fact]
    public void MongoConnectorException_MessageAndInner_StoresInnerException()
    {
        var inner = new InvalidOperationException("inner");
        var ex = new MongoConnectorException("test error", inner);
        ex.Message.Should().Be("test error");
        ex.InnerException.Should().BeSameAs(inner);
    }

    [Fact]
    public void MongoConnectorException_WithContext_StoresCollectionAndOperation()
    {
        var ex = new MongoConnectorException("test error", "my_collection", "Write");
        ex.Message.Should().Be("test error");
        ex.CollectionName.Should().Be("my_collection");
        ex.OperationContext.Should().Be("Write");
        ex.InnerException.Should().BeNull();
    }

    [Fact]
    public void MongoConnectorException_WithContextAndInner_StoresAll()
    {
        var inner = new TimeoutException("timeout");
        var ex = new MongoConnectorException("test error", "my_collection", "Read", inner);
        ex.CollectionName.Should().Be("my_collection");
        ex.OperationContext.Should().Be("Read");
        ex.InnerException.Should().BeSameAs(inner);
    }

    [Fact]
    public void MongoConnectorException_IsTransient_DefaultsFalse()
    {
        var ex = new MongoConnectorException("error");
        ex.IsTransient.Should().BeFalse();
    }

    [Fact]
    public void MongoConnectorException_IsTransient_CanBeSetViaInit()
    {
        var ex = new MongoConnectorException("error") { IsTransient = true };
        ex.IsTransient.Should().BeTrue();
    }

    [Fact]
    public void MongoConnectorException_NullCollectionAndContext_AreAllowed()
    {
        var ex = new MongoConnectorException("error", null, null);
        ex.CollectionName.Should().BeNull();
        ex.OperationContext.Should().BeNull();
    }
}

public class MongoMappingExceptionTests
{
    [Fact]
    public void MongoMappingException_MessageOnly_StoresMessage()
    {
        var ex = new MongoMappingException("mapping failed");
        ex.Message.Should().Be("mapping failed");
        ex.OffendingDocument.Should().BeNull();
        ex.InnerException.Should().BeNull();
    }

    [Fact]
    public void MongoMappingException_MessageAndInner_StoresInnerException()
    {
        var inner = new InvalidCastException("cast");
        var ex = new MongoMappingException("mapping failed", inner);
        ex.InnerException.Should().BeSameAs(inner);
    }

    [Fact]
    public void MongoMappingException_WithFieldName_StoresFieldName()
    {
        var ex = new MongoMappingException("mapping failed", "age");
        ex.Message.Should().Be("mapping failed");
    }

    [Fact]
    public void MongoMappingException_WithFieldNameAndInner_StoresAll()
    {
        var inner = new FormatException("format");
        var ex = new MongoMappingException("mapping failed", "created_at", inner);
        ex.InnerException.Should().BeSameAs(inner);
    }

    [Fact]
    public void MongoMappingException_WithOffendingDocument_StoresDocument()
    {
        var doc = new BsonDocument { ["name"] = "Alice", ["age"] = 30 };
        var ex = new MongoMappingException("mapping failed", "age", doc);
        ex.OffendingDocument.Should().BeSameAs(doc);
    }

    [Fact]
    public void MongoMappingException_WithOffendingDocumentAndInner_StoresAll()
    {
        var doc = new BsonDocument { ["x"] = 1 };
        var inner = new OverflowException("overflow");
        var ex = new MongoMappingException("mapping failed", "x", doc, inner);
        ex.OffendingDocument.Should().BeSameAs(doc);
        ex.InnerException.Should().BeSameAs(inner);
    }

    [Fact]
    public void MongoMappingException_NullDocument_IsAllowed()
    {
        var ex = new MongoMappingException("mapping failed", "field", offendingDocument: null);
        ex.OffendingDocument.Should().BeNull();
    }
}

public class MongoWriteExceptionTests
{
    [Fact]
    public void MongoWriteException_MessageOnly_StoresMessage()
    {
        var ex = new MongoWriteException("write failed");
        ex.Message.Should().Be("write failed");
        ex.CollectionName.Should().BeNull();
        ex.FailedBatchCount.Should().Be(0);
        ex.SuccessfullyWrittenCount.Should().Be(0);
        ex.InnerException.Should().BeNull();
    }

    [Fact]
    public void MongoWriteException_MessageAndInner_StoresInnerException()
    {
        var inner = new IOException("io");
        var ex = new MongoWriteException("write failed", inner);
        ex.InnerException.Should().BeSameAs(inner);
    }

    [Fact]
    public void MongoWriteException_WithBatchInfo_StoresBatchDetails()
    {
        var ex = new MongoWriteException("write failed", "events", 250);
        ex.CollectionName.Should().Be("events");
        ex.FailedBatchCount.Should().Be(250);
        ex.InnerException.Should().BeNull();
    }

    [Fact]
    public void MongoWriteException_WithBatchInfoAndInner_StoresAll()
    {
        var inner = new TimeoutException("timeout");
        var ex = new MongoWriteException("write failed", "events", 100, inner);
        ex.CollectionName.Should().Be("events");
        ex.FailedBatchCount.Should().Be(100);
        ex.InnerException.Should().BeSameAs(inner);
    }

    [Fact]
    public void MongoWriteException_SuccessfullyWrittenCount_CanBeSetViaInit()
    {
        var ex = new MongoWriteException("write failed", "events", 50)
        {
            SuccessfullyWrittenCount = 950,
        };

        ex.SuccessfullyWrittenCount.Should().Be(950);
    }

    [Fact]
    public void MongoWriteException_NullCollection_IsAllowed()
    {
        var ex = new MongoWriteException("error", null, 0);
        ex.CollectionName.Should().BeNull();
    }
}
