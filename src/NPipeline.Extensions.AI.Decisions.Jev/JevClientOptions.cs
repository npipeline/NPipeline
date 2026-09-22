namespace NPipeline.Extensions.AI.Decisions.Jev;

/// <summary>Configures the TypeSafe System One HTTP client.</summary>
public sealed record JevClientOptions
{
    /// <summary>Gets the TypeSafe API key.</summary>
    public required string ApiKey { get; init; }

    /// <summary>Gets the TypeSafe API root.</summary>
    public Uri BaseUri { get; init; } = new("https://api.typesafe.ai", UriKind.Absolute);

    /// <summary>Gets the model used when a request does not override it.</summary>
    public string DefaultModel { get; init; } = "jev-latest";

    /// <summary>Gets the timeout applied to each HTTP attempt.</summary>
    public TimeSpan AttemptTimeout { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>Gets the retry policy.</summary>
    public JevRetryPolicy Retry { get; init; } = new();

    /// <summary>Gets the time provider used for retry delays.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}

/// <summary>Configures retries for transient TypeSafe API failures.</summary>
public sealed record JevRetryPolicy
{
    /// <summary>Gets the maximum retries after the initial attempt.</summary>
    public int MaxRetries { get; init; } = 2;

    /// <summary>Gets the first exponential-backoff delay.</summary>
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromMilliseconds(500);

    /// <summary>Gets the maximum exponential-backoff delay.</summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Gets the maximum accepted server-specified retry delay.</summary>
    public TimeSpan MaxRetryAfter { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>Gets the fraction of a backoff delay that can be subtracted as jitter.</summary>
    public double JitterFactor { get; init; } = 0.25;
}
