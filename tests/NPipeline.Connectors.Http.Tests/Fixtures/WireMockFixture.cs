using WireMock.Server;
using WireMock.Settings;

namespace NPipeline.Connectors.Http.Tests.Fixtures;

/// <summary>
///     Starts a WireMock.Net in-process HTTP server on a random port for integration tests.
///     Shared across all tests in the <see cref="HttpTestFixture" /> collection.
/// </summary>
public sealed class WireMockFixture : IDisposable
{
    public WireMockFixture()
    {
        Server = WireMockServer.Start(new WireMockServerSettings
        {
            UseSSL = false,
            StartAdminInterface = false,
        });
    }

    public WireMockServer Server { get; }

    public string BaseUrl => Server.Url!;

    public void Dispose()
    {
        Server.Stop();
        Server.Dispose();
    }
}
