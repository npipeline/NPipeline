using MongoDB.Bson;
using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Configuration;
using NPipeline.Connectors.MongoDB.Writers;

namespace NPipeline.Connectors.MongoDB.Tests.Configuration;

public class MongoWriteStrategyTests
{
    [Fact]
    public void MongoWriterFactory_Create_InsertMany_ReturnsInsertManyWriter()
    {
        var writer = MongoWriterFactory.Create<SimpleRecord>(MongoWriteStrategy.InsertMany);
        writer.Should().BeOfType<MongoInsertManyWriter<SimpleRecord>>();
    }

    [Fact]
    public void MongoWriterFactory_Create_BulkWrite_ReturnsBulkWriter()
    {
        var writer = MongoWriterFactory.Create<SimpleRecord>(MongoWriteStrategy.BulkWrite);
        writer.Should().BeOfType<MongoBulkWriter<SimpleRecord>>();
    }

    [Fact]
    public void MongoWriterFactory_Create_Upsert_ReturnsUpsertWriter()
    {
        var writer = MongoWriterFactory.Create<SimpleRecord>(MongoWriteStrategy.Upsert);
        writer.Should().BeOfType<MongoUpsertWriter<SimpleRecord>>();
    }

    [Fact]
    public void MongoWriterFactory_Create_UnknownStrategy_ThrowsArgumentOutOfRangeException()
    {
        var act = () => MongoWriterFactory.Create<SimpleRecord>((MongoWriteStrategy)99);
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void MongoWriterFactory_CreateFromConfiguration_UsesConfigurationStrategy()
    {
        var config = new MongoConfiguration
        {
            DatabaseName = "test",
            CollectionName = "col",
            WriteStrategy = MongoWriteStrategy.InsertMany,
        };

        var writer = MongoWriterFactory.CreateFromConfiguration<SimpleRecord>(config);
        writer.Should().BeOfType<MongoInsertManyWriter<SimpleRecord>>();
    }

    [Fact]
    public void MongoWriterFactory_CreateFromConfiguration_NullConfiguration_Throws()
    {
        var act = () => MongoWriterFactory.CreateFromConfiguration<SimpleRecord>(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void MongoWriterFactory_Create_WithCustomMapper_WiresMapperIntoWriter()
    {
        Func<SimpleRecord, BsonDocument> mapper = r => new BsonDocument("x", r.Name);
        var writer = MongoWriterFactory.Create<SimpleRecord>(MongoWriteStrategy.BulkWrite, mapper);
        writer.Should().BeOfType<MongoBulkWriter<SimpleRecord>>();
    }

    [Fact]
    public void MongoWriterFactory_Create_UpsertWithFilterBuilder_WiresFilterBuilder()
    {
        Func<SimpleRecord, FilterDefinition<BsonDocument>> filter =
            r => Builders<BsonDocument>.Filter.Eq("name", r.Name);

        var writer = MongoWriterFactory.Create<SimpleRecord>(
            MongoWriteStrategy.Upsert,
            upsertFilterBuilder: filter);

        writer.Should().BeOfType<MongoUpsertWriter<SimpleRecord>>();
    }

    [Theory]
    [InlineData(MongoWriteStrategy.InsertMany)]
    [InlineData(MongoWriteStrategy.BulkWrite)]
    [InlineData(MongoWriteStrategy.Upsert)]
    public void MongoWriteStrategy_AllValuesAreKnown(MongoWriteStrategy strategy)
    {
        Enum.IsDefined(strategy).Should().BeTrue();
    }

    private sealed class SimpleRecord
    {
        public string Name { get; } = string.Empty;
    }
}
