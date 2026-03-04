using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using NPipeline.Connectors.MongoDB.Attributes;
using NPipeline.Connectors.MongoDB.Mapping;

namespace NPipeline.Connectors.MongoDB.Tests.Mapping;

/// <summary>
///     Unit tests for <see cref="MongoDocumentMapper" /> — high-level mapping API.
/// </summary>
public sealed class MongoDocumentMapperTests
{
    [Fact]
    public void Map_MapsDocumentToTypeUsingAttributes()
    {
        var doc = new BsonDocument
        {
            { "_id", "order-1" },
            { "name", "Test Order" },
            { "total", new BsonDecimal128(99.99m) },
        };

        var result = MongoDocumentMapper.Map<Order>(new MongoRow(doc));

        result.Id.Should().Be("order-1");
        result.Name.Should().Be("Test Order");
        result.Total.Should().Be(99.99m);
    }

    [Fact]
    public void Map_ThrowsOnNullRow()
    {
        var act = () => MongoDocumentMapper.Map<Order>(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void TryMap_ReturnsTrueOnSuccess()
    {
        var doc = new BsonDocument { { "_id", "x" }, { "name", "Y" } };
        var row = new MongoRow(doc);

        MongoDocumentMapper.TryMap<Order>(row, out var result).Should().BeTrue();
        result.Should().NotBeNull();
        result!.Id.Should().Be("x");
    }

    [Fact]
    public void TryMap_ReturnsFalseOnNullRow()
    {
        MongoDocumentMapper.TryMap<Order>(null!, out var result).Should().BeFalse();
        result.Should().BeNull();
    }

    [Fact]
    public void MapAll_MapsAllRows()
    {
        var rows = Enumerable.Range(1, 5)
            .Select(i => new MongoRow(new BsonDocument { { "_id", $"id-{i}" }, { "name", $"Order {i}" } }))
            .ToList();

        var results = MongoDocumentMapper.MapAll<Order>(rows).ToList();

        results.Should().HaveCount(5);
        results[0].Id.Should().Be("id-1");
        results[4].Id.Should().Be("id-5");
    }

    [Fact]
    public void MapAll_ThrowsOnNullRows()
    {
        var act = () => MongoDocumentMapper.MapAll<Order>(null!).ToList();
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void Map_UsesCamelCaseFallbackConvention()
    {
        var doc = new BsonDocument { { "firstName", "Jane" }, { "age", 25 } };
        var row = new MongoRow(doc);

        var result = MongoDocumentMapper.Map<ConventionMapped>(row);

        result.FirstName.Should().Be("Jane");
        result.Age.Should().Be(25);
    }

    [Fact]
    public void Map_UsesBsonElementAttributeAsFallback()
    {
        var dob = new DateTime(1990, 6, 15, 0, 0, 0, DateTimeKind.Utc);
        var doc = new BsonDocument { { "full_name", "John Doe" }, { "dob", new BsonDateTime(dob) } };

        var result = MongoDocumentMapper.Map<BsonElementMapped>(new MongoRow(doc));

        result.FullName.Should().Be("John Doe");
        result.DateOfBirth.Should().Be(dob);
    }

    [MongoCollection("orders")]
    private sealed class Order
    {
        [MongoField("_id")]
        public string Id { get; set; } = "";

        [MongoField("name")]
        public string Name { get; set; } = "";

        [MongoField("total")]
        public decimal Total { get; set; }
    }

    private sealed class ConventionMapped
    {
        public string FirstName { get; set; } = "";
        public int Age { get; set; }
    }

    private sealed class BsonElementMapped
    {
        [BsonElement("full_name")]
        public string FullName { get; set; } = "";

        [BsonElement("dob")]
        public DateTime DateOfBirth { get; set; }
    }
}
