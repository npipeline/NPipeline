using AwesomeAssertions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Tests;

public sealed class StorageSchemeTests
{
    [Fact]
    public void Ctor_WithMixedCase_NormalizesToLowercase()
    {
        var s = new StorageScheme("FiLe");
        s.Value.Should().Be("file");
        s.ToString().Should().Be("file");
    }

    [Fact]
    public void Conversions_ToAndFromString_RoundTrip()
    {
        var s = (StorageScheme)"s3";
        string str = s;
        str.Should().Be("s3");
        s.Should().Be(new StorageScheme("S3")); // case-insensitive equality via normalization
    }

    [Fact]
    public void IsValid_WithInvalidFirstChar_ReturnsFalse()
    {
        StorageScheme.IsValid("1bad").Should().BeFalse();
        StorageScheme.IsValid("+bad").Should().BeFalse();
        StorageScheme.IsValid("-bad").Should().BeFalse();
        StorageScheme.IsValid(".bad").Should().BeFalse();
    }

    [Fact]
    public void Ctor_WithInvalidChars_ThrowsArgumentException()
    {
        Action act = () => _ = new StorageScheme("az*re");

        act.Should().Throw<ArgumentException>()
            .WithMessage("*Invalid scheme*");
    }

    [Fact]
    public void TryParse_WithNullOrWhitespace_ReturnsFalse()
    {
        StorageScheme.TryParse(null, out _).Should().BeFalse();
        StorageScheme.TryParse("  ", out _).Should().BeFalse();
    }

    [Fact]
    public void TryParse_WithValidValue_ReturnsTrueAndNormalized()
    {
        var ok = StorageScheme.TryParse("Az-09+", out var scheme);
        ok.Should().BeTrue();
        scheme.ToString().Should().Be("az-09+");
    }

    [Fact]
    public void WellKnownSchemes_AreCached()
    {
        ReferenceEquals(StorageScheme.File.Value, StorageScheme.File.Value).Should().BeTrue();
        StorageScheme.S3.Value.Should().BeSameAs(StorageScheme.S3.Value);
        StorageScheme.Gcs.Value.Should().Be("gs");
    }

    [Fact]
    public void Default_ValueIsEmptyString()
    {
        var scheme = default(StorageScheme);

        scheme.Value.Should().BeEmpty();
        scheme.ToString().Should().BeEmpty();
        ((string)scheme).Should().BeEmpty();
    }

    [Fact]
    public void ExplicitConversion_WithInvalidValue_Throws()
    {
        Action act = () => _ = (StorageScheme)"1bad";

        act.Should().Throw<ArgumentException>();
    }
}
