using NPipeline.Connectors.Aws.Redshift.Mapping;

namespace NPipeline.Connectors.Aws.Redshift.Tests.Mapping;

public class RedshiftParameterMapperTests : IDisposable
{
    public RedshiftParameterMapperTests()
    {
        RedshiftParameterMapper.ClearCache();
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        RedshiftParameterMapper.ClearCache();
    }

    [Fact]
    public void BuildValueExtractor_ExtractsAllProperties()
    {
        // Arrange
        var item = new TestPoco
        {
            Id = 1,
            Name = "Test",
            Price = 99.99m,
            Quantity = 10,
            CreatedAt = new DateTime(2024, 1, 1),
            IsActive = true,
        };

        // Act
        var extractor = RedshiftParameterMapper.BuildValueExtractor<TestPoco>();
        var result = extractor(item);

        // Assert
        result.Should().HaveCount(6);
        result[0].Should().Be(1);
        result[1].Should().Be("Test");
        result[2].Should().Be(99.99m);
        result[3].Should().Be(10);
        result[4].Should().Be(new DateTime(2024, 1, 1));
        result[5].Should().Be(true);
    }

    [Fact]
    public void BuildValueExtractor_WithNullValues_ExtractsNulls()
    {
        // Arrange
        var item = new NullablePoco
        {
            Id = 1,
            Name = null,
            NullableInt = null,
        };

        // Act
        var extractor = RedshiftParameterMapper.BuildValueExtractor<NullablePoco>();
        var result = extractor(item);

        // Assert
        result.Should().HaveCount(3);
        result[0].Should().Be(1);
        result[1].Should().BeNull();
        result[2].Should().BeNull();
    }

    [Fact]
    public void BuildValueExtractor_WithIgnoredProperty_SkipsProperty()
    {
        // Arrange
        var item = new IgnorePoco
        {
            Id = 1,
            Ignored = "should not be extracted",
            Name = "Test",
        };

        // Act
        var extractor = RedshiftParameterMapper.BuildValueExtractor<IgnorePoco>();
        var result = extractor(item);

        // Assert
        result.Should().HaveCount(2); // Id and Name, Ignored should be skipped
        result[0].Should().Be(1);
        result[1].Should().Be("Test");
    }

    [Fact]
    public void GetColumnNames_ReturnsCorrectNames()
    {
        // Act
        var names = RedshiftParameterMapper.GetColumnNames<TestPoco>();

        // Assert
        names.Should().ContainInOrder("id", "name", "price", "quantity", "created_at", "is_active");
    }

    [Fact]
    public void GetColumnNames_WithAttribute_UsesAttributeName()
    {
        // Act
        var names = RedshiftParameterMapper.GetColumnNames<AttributePoco>();

        // Assert
        names.Should().Contain("custom_column_name");
    }

    [Fact]
    public void GetColumnNames_WithIgnoredProperty_SkipsProperty()
    {
        // Act
        var names = RedshiftParameterMapper.GetColumnNames<IgnorePoco>();

        // Assert
        names.Should().ContainInOrder("id", "name");
        names.Should().NotContain("ignored");
    }

    [Fact]
    public void BuildValueExtractor_CachesMapper_SameTypeReturnsSameDelegate()
    {
        // Act
        var extractor1 = RedshiftParameterMapper.BuildValueExtractor<TestPoco>();
        var extractor2 = RedshiftParameterMapper.BuildValueExtractor<TestPoco>();

        // Assert
        extractor1.Should().BeSameAs(extractor2);
    }

    [Fact]
    public void GetColumnNames_WithLowercaseConvention_ReturnsLowercase()
    {
        // Act
        var names = RedshiftParameterMapper.GetColumnNames<TestPoco>(RedshiftNamingConvention.Lowercase);

        // Assert
        names.Should().ContainInOrder("id", "name", "price", "quantity", "createdat", "isactive");
    }

    [Fact]
    public void GetColumnNames_WithAsIsConvention_ReturnsOriginalNames()
    {
        // Act
        var names = RedshiftParameterMapper.GetColumnNames<TestPoco>(RedshiftNamingConvention.AsIs);

        // Assert
        names.Should().ContainInOrder("Id", "Name", "Price", "Quantity", "CreatedAt", "IsActive");
    }

    private sealed class TestPoco
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public int Quantity { get; set; }
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
    }

    private sealed class NullablePoco
    {
        public int Id { get; set; }
        public string? Name { get; set; }
        public int? NullableInt { get; set; }
    }

    private sealed class IgnorePoco
    {
        public int Id { get; set; }

        [RedshiftColumn("ignored", Ignore = true)]
        public string? Ignored { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class AttributePoco
    {
        [RedshiftColumn("custom_column_name")]
        public string CustomProperty { get; set; } = string.Empty;
    }
}
