using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using AwesomeAssertions;
using NPipeline.ErrorHandling;
using NPipeline.Reliability;

namespace NPipeline.Tests.Reliability;

[SuppressMessage("Usage", "xUnit1045", Justification = "Exception rows are built in code and never need to be serialized.")]
public sealed class RetryClassifierTests
{
    public static TheoryData<Exception> TransientFailures =>
    [
        new TimeoutException(),
        new IOException(),
        new SocketException(),
        new HttpRequestException("no status"),
        new HttpRequestException("timeout", null, HttpStatusCode.RequestTimeout),
        new HttpRequestException("throttled", null, HttpStatusCode.TooManyRequests),
        new HttpRequestException("unavailable", null, HttpStatusCode.ServiceUnavailable),
        new TaskCanceledException("client timeout"),
    ];

    public static TheoryData<Exception> PermanentFailures =>
    [
        new InvalidOperationException(),
        new ArgumentException(),
        new FormatException(),
        new HttpRequestException("not found", null, HttpStatusCode.NotFound),
        new RetryExhaustedException("node", 3, new TimeoutException()),
    ];

    [Theory]
    [MemberData(nameof(TransientFailures))]
    public void Default_TreatsInfrastructureFailuresAsTransient(Exception exception)
    {
        RetryClassifier.Default.IsTransient(exception, CancellationToken.None).Should().BeTrue();
    }

    [Theory]
    [MemberData(nameof(PermanentFailures))]
    public void Default_TreatsEverythingElseAsPermanent(Exception exception)
    {
        RetryClassifier.Default.IsTransient(exception, CancellationToken.None).Should().BeFalse();
    }

    [Fact]
    public void Default_LooksThroughThePipelinesWrappers()
    {
        var wrapped = new NodeExecutionException("node", "failed",
            new TargetInvocationException(new AggregateException(new TimeoutException())));

        RetryClassifier.Default.IsTransient(wrapped, CancellationToken.None).Should().BeTrue();
    }

    [Fact]
    public void Default_DoesNotLookThroughAnAggregateOfSeveralFailures()
    {
        var aggregate = new AggregateException(new TimeoutException(), new TimeoutException());

        RetryClassifier.Default.IsTransient(aggregate, CancellationToken.None).Should().BeFalse();
    }

    [Fact]
    public void Default_TreatsAFailureNResilienceAlreadyRetriedAsPermanent()
    {
        var exception = new TimeoutException();
        exception.Data["NResilience.Attempts"] = 3;

        RetryClassifier.Default.IsTransient(new NodeExecutionException("node", "failed", exception), CancellationToken.None).Should().BeFalse();
    }

    [Fact]
    public void PipelineCancellation_IsNeverTransient_EvenUnderAll()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        RetryClassifier.Default.IsTransient(new TaskCanceledException(), cts.Token).Should().BeFalse();
        RetryClassifier.All.IsTransient(new OperationCanceledException(cts.Token), cts.Token).Should().BeFalse();
        RetryClassifier.All.Transient<OperationCanceledException>().IsTransient(new OperationCanceledException(), cts.Token).Should().BeFalse();
    }

    [Fact]
    public void All_TreatsEveryFailureAsTransient()
    {
        RetryClassifier.All.IsTransient(new InvalidOperationException(), CancellationToken.None).Should().BeTrue();
    }

    [Fact]
    public void Rules_AreCheckedBeforeTheBuiltInOnes_InTheOrderAdded()
    {
        var classifier = RetryClassifier.Default
            .Permanent<TimeoutException>(e => e.Message == "fatal")
            .Transient<TimeoutException>()
            .Transient<InvalidOperationException>(e => e.Message == "stale");

        classifier.IsTransient(new TimeoutException("fatal"), CancellationToken.None).Should().BeFalse();
        classifier.IsTransient(new TimeoutException("slow"), CancellationToken.None).Should().BeTrue();
        classifier.IsTransient(new InvalidOperationException("stale"), CancellationToken.None).Should().BeTrue();
        classifier.IsTransient(new InvalidOperationException("bug"), CancellationToken.None).Should().BeFalse();
    }

    [Fact]
    public void Rules_MatchWrappedFailures()
    {
        var classifier = RetryClassifier.Default.Transient<InvalidOperationException>();

        classifier.IsTransient(new NodeExecutionException("node", "failed", new InvalidOperationException()), CancellationToken.None)
            .Should().BeTrue();
    }

    [Fact]
    public void AddingARule_ReturnsANewClassifier()
    {
        var withRule = RetryClassifier.Default.Transient<InvalidOperationException>();

        withRule.Should().NotBeSameAs(RetryClassifier.Default);
        RetryClassifier.Default.IsTransient(new InvalidOperationException(), CancellationToken.None).Should().BeFalse();
    }
}
