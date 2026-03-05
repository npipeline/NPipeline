using System.Reflection;
using System.Runtime.ExceptionServices;
using NPipeline.Connectors.Http.Configuration;

namespace NPipeline.Connectors.Http.Tests.Configuration;

public class HttpSinkConfigurationTests
{
    private static HttpSinkConfiguration ValidConfig()
    {
        return new HttpSinkConfiguration { Uri = new Uri("https://api.example.com/items") };
    }

    [Fact]
    public void Validate_WithStaticUri_DoesNotThrow()
    {
        var config = ValidConfig();
        var act = () => InvokeValidate(config);
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_WithUriFactory_DoesNotThrow()
    {
        var config = new HttpSinkConfiguration { UriFactory = _ => new Uri("https://api.example.com/items/1") };
        var act = () => InvokeValidate(config);
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_WithNeitherUriNorUriFactory_ThrowsArgumentException()
    {
        // Can't use required init on Uri since there are two optional-but-one-required props
        // Construct via reflection to bypass required keyword issue
        var config = new HttpSinkConfiguration { Uri = new Uri("https://placeholder.com") };

        // Force both to null via a helper config class — we test via ctor validation instead
        // The constructor is validated internally, so create a valid one and then test the edge
        var badConfig = new HttpSinkConfiguration { Uri = null! };
        var act = () => InvokeValidate(badConfig);

        act.Should().Throw<ArgumentException>()
            .WithMessage("*Uri*");
    }

    [Fact]
    public void Validate_WithBatchSizeZero_ThrowsArgumentException()
    {
        var config = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com"),
            BatchSize = 0,
        };

        var act = () => InvokeValidate(config);

        act.Should().Throw<ArgumentException>()
            .WithMessage("*BatchSize*");
    }

    [Fact]
    public void Validate_WithZeroTimeout_ThrowsArgumentException()
    {
        var config = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com"),
            Timeout = TimeSpan.Zero,
        };

        var act = () => InvokeValidate(config);

        act.Should().Throw<ArgumentException>()
            .WithMessage("*Timeout*");
    }

    [Fact]
    public void Validate_WithIdempotencyKeyFactoryAndEmptyHeaderName_ThrowsArgumentException()
    {
        var config = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com"),
            IdempotencyKeyFactory = _ => "key",
            IdempotencyHeaderName = "",
        };

        var act = () => InvokeValidate(config);

        act.Should().Throw<ArgumentException>()
            .WithMessage("*IdempotencyHeaderName*");
    }

    [Fact]
    public void Validate_WithIdempotencyKeyFactoryAndValidHeaderName_DoesNotThrow()
    {
        var config = new HttpSinkConfiguration
        {
            Uri = new Uri("https://api.example.com"),
            IdempotencyKeyFactory = _ => "key",
            IdempotencyHeaderName = "Idempotency-Key",
        };

        var act = () => InvokeValidate(config);
        act.Should().NotThrow();
    }

    private static void InvokeValidate(HttpSinkConfiguration config)
    {
        try
        {
            config.GetType()
                .GetMethod("Validate", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(config, null);
        }
        catch (TargetInvocationException tie) when (tie.InnerException != null)
        {
            ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
        }
    }
}
