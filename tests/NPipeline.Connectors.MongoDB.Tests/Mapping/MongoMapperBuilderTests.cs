using MongoDB.Bson;
using NPipeline.Connectors.MongoDB.Attributes;
using NPipeline.Connectors.MongoDB.Exceptions;
using NPipeline.Connectors.MongoDB.Mapping;

namespace NPipeline.Connectors.MongoDB.Tests.Mapping;

/// <summary>
///     Unit tests for <see cref="MongoMapperBuilder" /> — compiled mapper caching.
/// </summary>
public sealed class MongoMapperBuilderTests
{
    [Fact]
    public void GetOrCreateMapper_ReturnsSameInstanceOnSubsequentCalls()
    {
        var mapper1 = MongoMapperBuilder.GetOrCreateMapper<SimpleModel>();
        var mapper2 = MongoMapperBuilder.GetOrCreateMapper<SimpleModel>();

        // Same cached delegate instance
        ReferenceEquals(mapper1, mapper2).Should().BeTrue();
    }

    [Fact]
    public void Build_MapsFieldsUsingMongoFieldAttribute()
    {
        var doc = new BsonDocument { { "name", "WidgetA" }, { "qty", 10 } };
        var mapper = MongoMapperBuilder.Build<SimpleModel>();

        var result = mapper(new MongoRow(doc));

        result.Name.Should().Be("WidgetA");
        result.Quantity.Should().Be(10);
    }

    [Fact]
    public void Build_IgnoresMissingFieldsGracefully()
    {
        // Only 'qty' supplied, 'name' is missing — should leave it as default ("")
        var doc = new BsonDocument { { "qty", 5 } };
        var mapper = MongoMapperBuilder.Build<SimpleModel>();

        var result = mapper(new MongoRow(doc));

        result.Name.Should().Be("");
        result.Quantity.Should().Be(5);
    }

    [Fact]
    public void Build_ThrowsMongoMappingExceptionOnTypeMismatch()
    {
        // "qty" is a nested document, not an int — should throw MongoMappingException
        var doc = new BsonDocument { { "qty", new BsonDocument { { "x", 1 } } } };
        var mapper = MongoMapperBuilder.Build<SimpleModel>();

        var act = () => mapper(new MongoRow(doc));

        act.Should().Throw<MongoMappingException>();
    }

    [Fact]
    public void Build_HandlesDecimalBsonDecimal128()
    {
        var doc = new BsonDocument
        {
            { "value", new BsonDecimal128(123.45m) },
            { "flag", true },
            { "ts", new BsonDateTime(new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc)) },
        };

        var mapper = MongoMapperBuilder.Build<NestedModel>();
        var result = mapper(new MongoRow(doc));

        result.Value.Should().Be(123.45m);
        result.IsActive.Should().BeTrue();
        result.Timestamp.Year.Should().Be(2024);
    }

    [Fact]
    public void ClearCache_CausesNewMapperToBeBuilt()
    {
        var mapper1 = MongoMapperBuilder.GetOrCreateMapper<SimpleModel>();
        MongoMapperBuilder.ClearCache();
        var mapper2 = MongoMapperBuilder.GetOrCreateMapper<SimpleModel>();

        // After clearing, a new delegate is compiled
        ReferenceEquals(mapper1, mapper2).Should().BeFalse();
    }

    [Fact]
    public void Build_SkipsReadOnlyProperties()
    {
        var doc = new BsonDocument { { "writable", "yes" }, { "readOnly", "ignored" } };
        var mapper = MongoMapperBuilder.Build<ReadOnlyPropertyModel>();

        var result = mapper(new MongoRow(doc));

        result.Writable.Should().Be("yes");
        result.ReadOnly.Should().Be("always");
    }

    private sealed class SimpleModel
    {
        [MongoField("name")]
        public string Name { get; set; } = "";

        [MongoField("qty")]
        public int Quantity { get; set; }
    }

    private sealed class NestedModel
    {
        [MongoField("value")]
        public decimal Value { get; set; }

        [MongoField("flag")]
        public bool IsActive { get; set; }

        [MongoField("ts")]
        public DateTime Timestamp { get; set; }
    }

    private sealed class ReadOnlyPropertyModel
    {
        // Read-only — should be skipped
        public string ReadOnly => "always";

        [MongoField("writable")]
        public string Writable { get; set; } = "";
    }
}
