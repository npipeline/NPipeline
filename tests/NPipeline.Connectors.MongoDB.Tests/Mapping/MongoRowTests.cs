using MongoDB.Bson;
using NPipeline.Connectors.MongoDB.Mapping;

namespace NPipeline.Connectors.MongoDB.Tests.Mapping;

/// <summary>
///     Unit tests for <see cref="MongoRow" /> typed accessor methods.
/// </summary>
public sealed class MongoRowTests
{
    [Fact]
    public void Get_ReturnsStringValue()
    {
        var doc = new BsonDocument { { "name", "Alice" } };
        var row = new MongoRow(doc);

        row.Get<string>("name").Should().Be("Alice");
    }

    [Fact]
    public void Get_ReturnsInt32Value()
    {
        var doc = new BsonDocument { { "age", 30 } };
        var row = new MongoRow(doc);

        row.Get<int>("age").Should().Be(30);
    }

    [Fact]
    public void Get_ReturnsInt64Value()
    {
        var doc = new BsonDocument { { "count", new BsonInt64(1_000_000L) } };
        var row = new MongoRow(doc);

        row.Get<long>("count").Should().Be(1_000_000L);
    }

    [Fact]
    public void Get_ReturnsDoubleValue()
    {
        var doc = new BsonDocument { { "score", 3.14 } };
        var row = new MongoRow(doc);

        row.Get<double>("score").Should().BeApproximately(3.14, 0.001);
    }

    [Fact]
    public void Get_ReturnsBoolValue()
    {
        var doc = new BsonDocument { { "active", true } };
        var row = new MongoRow(doc);

        row.Get<bool>("active").Should().BeTrue();
    }

    [Fact]
    public void Get_ReturnsDateTimeValue()
    {
        var utcNow = DateTime.UtcNow.Date;
        var doc = new BsonDocument { { "createdAt", new BsonDateTime(utcNow) } };
        var row = new MongoRow(doc);

        row.Get<DateTime>("createdAt").Should().Be(utcNow);
    }

    [Fact]
    public void Get_ReturnsDefaultForMissingField()
    {
        var doc = new BsonDocument();
        var row = new MongoRow(doc);

        row.Get<string>("missing", "default").Should().Be("default");
    }

    [Fact]
    public void Get_ReturnsDefaultForNullField()
    {
        var doc = new BsonDocument { { "field", BsonNull.Value } };
        var row = new MongoRow(doc);

        row.Get<string>("field", "fallback").Should().Be("fallback");
    }

    [Fact]
    public void HasField_ReturnsTrueWhenFieldExists()
    {
        var doc = new BsonDocument { { "x", 1 } };
        var row = new MongoRow(doc);

        row.HasField("x").Should().BeTrue();
    }

    [Fact]
    public void HasField_ReturnsFalseWhenFieldMissing()
    {
        var doc = new BsonDocument();
        var row = new MongoRow(doc);

        row.HasField("missing").Should().BeFalse();
    }

    [Fact]
    public void GetString_ReturnsStringValue()
    {
        var doc = new BsonDocument { { "name", "Bob" } };
        var row = new MongoRow(doc);

        row.GetString("name").Should().Be("Bob");
    }

    [Fact]
    public void GetInt32_ReturnsIntValue()
    {
        var doc = new BsonDocument { { "qty", 42 } };
        var row = new MongoRow(doc);

        row.GetInt32("qty").Should().Be(42);
    }

    [Fact]
    public void GetBoolean_ReturnsBoolValue()
    {
        var doc = new BsonDocument { { "flag", false } };
        var row = new MongoRow(doc);

        row.GetBoolean("flag").Should().BeFalse();
    }

    [Fact]
    public void GetDocument_ReturnsNestedDocument()
    {
        var nested = new BsonDocument { { "city", "London" } };
        var doc = new BsonDocument { { "address", nested } };
        var row = new MongoRow(doc);

        var address = row.GetDocument("address");
        address.GetString("city").Should().Be("London");
    }

    [Fact]
    public void GetArray_ReturnsBsonArray()
    {
        var array = new BsonArray { "a", "b", "c" };
        var doc = new BsonDocument { { "tags", array } };
        var row = new MongoRow(doc);

        row.GetArray("tags").Count.Should().Be(3);
    }

    [Fact]
    public void Id_ReturnsObjectIdWhenPresent()
    {
        var oid = ObjectId.GenerateNewId();
        var doc = new BsonDocument { { "_id", oid } };
        var row = new MongoRow(doc);

        row.Id.Should().Be(oid);
    }

    [Fact]
    public void Id_ReturnsNullWhenIdIsMissing()
    {
        var doc = new BsonDocument();
        var row = new MongoRow(doc);

        row.Id.Should().BeNull();
    }

    [Fact]
    public void Id_ReturnsNullWhenIdIsNotObjectId()
    {
        // _id can be a string in MongoDB
        var doc = new BsonDocument { { "_id", "string-id" } };
        var row = new MongoRow(doc);

        row.Id.Should().BeNull();
    }

    [Fact]
    public void IsNullOrMissing_ReturnsTrueForMissingField()
    {
        var row = new MongoRow(new BsonDocument());
        row.IsNullOrMissing("x").Should().BeTrue();
    }

    [Fact]
    public void IsNullOrMissing_ReturnsTrueForNullField()
    {
        var doc = new BsonDocument { { "x", BsonNull.Value } };
        var row = new MongoRow(doc);
        row.IsNullOrMissing("x").Should().BeTrue();
    }

    [Fact]
    public void IsNullOrMissing_ReturnsFalseForPresentField()
    {
        var doc = new BsonDocument { { "x", 1 } };
        var row = new MongoRow(doc);
        row.IsNullOrMissing("x").Should().BeFalse();
    }

    [Fact]
    public void FieldCount_ReturnsCorrectCount()
    {
        var doc = new BsonDocument { { "a", 1 }, { "b", 2 }, { "c", 3 } };
        var row = new MongoRow(doc);
        row.FieldCount.Should().Be(3);
    }

    [Fact]
    public void FieldNames_ReturnsAllFieldNames()
    {
        var doc = new BsonDocument { { "x", 1 }, { "y", 2 } };
        var row = new MongoRow(doc);
        row.FieldNames.Should().Contain("x").And.Contain("y");
    }

    [Fact]
    public void Constructor_ThrowsOnNullDocument()
    {
        var act = () => new MongoRow(null!);
        act.Should().Throw<ArgumentNullException>().Which.ParamName.Should().Be("document");
    }

    [Fact]
    public void TryGet_ReturnsTrueAndValueWhenFieldExists()
    {
        var doc = new BsonDocument { { "qty", 7 } };
        var row = new MongoRow(doc);

        row.TryGet<int>("qty", out var value).Should().BeTrue();
        value.Should().Be(7);
    }

    [Fact]
    public void TryGet_ReturnsFalseAndDefaultWhenFieldMissing()
    {
        var row = new MongoRow(new BsonDocument());

        row.TryGet<int>("missing", out var value).Should().BeFalse();
        value.Should().Be(0);
    }
}
