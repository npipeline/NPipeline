using MongoDB.Bson;
using NPipeline.Connectors.MongoDB.Attributes;
using NPipeline.Connectors.MongoDB.Mapping;

namespace NPipeline.Connectors.MongoDB.Tests.Mapping;

/// <summary>
///     Unit tests for <see cref="MongoWriteDocumentMapper" /> — T → BsonDocument round-trip.
/// </summary>
public sealed class MongoWriteDocumentMapperTests
{
    [Fact]
    public void Map_ProducesCorrectBsonDocument()
    {
        var product = new Product { Id = "p-1", Name = "Widget", Price = 9.99m, Stock = 100 };

        var doc = MongoWriteDocumentMapper.Map(product);

        doc["_id"].AsString.Should().Be("p-1");
        doc["name"].AsString.Should().Be("Widget");
        doc["stock"].AsInt32.Should().Be(100);
    }

    [Fact]
    public void Map_ThrowsOnNullEntity()
    {
        var act = () => MongoWriteDocumentMapper.Map<Product>(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void MapAll_MapsMultipleEntities()
    {
        var products = new[]
        {
            new Product { Id = "p-1", Name = "A", Price = 1m, Stock = 1 },
            new Product { Id = "p-2", Name = "B", Price = 2m, Stock = 2 },
        };

        var docs = MongoWriteDocumentMapper.MapAll(products).ToList();

        docs.Should().HaveCount(2);
        docs[0]["_id"].AsString.Should().Be("p-1");
        docs[1]["_id"].AsString.Should().Be("p-2");
    }

    [Fact]
    public void GetOrCreateMapper_ReturnsSameInstance()
    {
        var m1 = MongoWriteDocumentMapper.GetOrCreateMapper<Product>();
        var m2 = MongoWriteDocumentMapper.GetOrCreateMapper<Product>();

        ReferenceEquals(m1, m2).Should().BeTrue();
    }

    [Fact]
    public void Map_WritesNullFieldAsBsonNull()
    {
        var entity = new WithNullable { Description = null };
        var doc = MongoWriteDocumentMapper.Map(entity);

        doc["desc"].Should().Be(BsonNull.Value);
    }

    [Fact]
    public void Map_UsesCamelCaseFallbackConvention()
    {
        var entity = new ConventionEntity
        {
            FirstName = "Charlie",
            CreatedAt = new DateTime(2025, 1, 15, 0, 0, 0, DateTimeKind.Utc),
        };

        var doc = MongoWriteDocumentMapper.Map(entity);

        doc["firstName"].AsString.Should().Be("Charlie");
        doc.Contains("createdAt").Should().BeTrue();
    }

    [MongoCollection("products")]
    private sealed class Product
    {
        [MongoField("_id")]
        public string Id { get; set; } = "";

        [MongoField("name")]
        public string Name { get; set; } = "";

        [MongoField("price")]
        public decimal Price { get; set; }

        [MongoField("stock")]
        public int Stock { get; set; }
    }

    private sealed class WithNullable
    {
        [MongoField("desc")]
        public string? Description { get; set; }
    }

    private sealed class ConventionEntity
    {
        public string FirstName { get; set; } = "";
        public DateTime CreatedAt { get; set; }
    }
}
