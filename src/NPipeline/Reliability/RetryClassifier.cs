using System.Collections.Immutable;
using System.Data.Common;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using NPipeline.ErrorHandling;

namespace NPipeline.Reliability;

/// <summary>
///     Decides whether a failure is transient, and so worth retrying, or permanent. Immutable: adding a rule returns a
///     new classifier.
/// </summary>
/// <remarks>
///     <para>
///         Rules you add are checked before the built-in ones, in the order you added them. A rule matches when the
///         exception, or any exception it wraps (see below), is of the rule's type and satisfies its condition.
///     </para>
///     <para>
///         Before judging, the classifier looks through the exceptions the pipeline wraps failures in:
///         <see cref="NodeExecutionException" />, <see cref="PipelineExecutionException" />, an
///         <see cref="AggregateException" /> with a single inner exception, and
///         <see cref="TargetInvocationException" />. Without this, every node failure would look permanent.
///     </para>
///     <para>
///         Cancellation of the pipeline's own token is never transient, whatever the rules say.
///     </para>
/// </remarks>
/// <example>
///     <code>
///     var classifier = RetryClassifier.Default
///         .Transient&lt;SqlException&gt;(e => SqlTransientErrors.Contains(e.Number))
///         .Permanent&lt;HttpRequestException&gt;(e => e.StatusCode == HttpStatusCode.NotImplemented);
///     </code>
/// </example>
public sealed class RetryClassifier
{
    /// <summary>
    ///     The key NResilience attaches to <see cref="Exception.Data" /> when it gives up. An exception carrying it has
    ///     already been retried by a connector, so retrying it again would multiply the connector's attempts.
    /// </summary>
    internal const string NResilienceAttemptsKey = "NResilience.Attempts";

    private readonly ImmutableArray<Rule> _rules;
    private readonly bool _everythingIsTransient;

    private RetryClassifier(ImmutableArray<Rule> rules, bool everythingIsTransient)
    {
        _rules = rules;
        _everythingIsTransient = everythingIsTransient;
    }

    /// <summary>
    ///     Treats as transient: <see cref="TimeoutException" />, <see cref="IOException" />,
    ///     <see cref="SocketException" />, an <see cref="HttpRequestException" /> with no status or with status 408,
    ///     429, or 5xx, a <see cref="DbException" /> whose <see cref="DbException.IsTransient" /> is true, and a
    ///     <see cref="TaskCanceledException" /> not caused by the pipeline's token (a client timeout). Everything else
    ///     is permanent, including a <see cref="RetryExhaustedException" /> and an exception that NResilience already
    ///     retried.
    /// </summary>
    public static RetryClassifier Default { get; } = new([], false);

    /// <summary>
    ///     Treats every exception as transient except cancellation of the pipeline's own token.
    /// </summary>
    public static RetryClassifier All { get; } = new([], true);

    /// <summary>
    ///     Returns a classifier that treats <typeparamref name="TException" /> as transient.
    /// </summary>
    /// <param name="when">An optional condition. When it returns false, the rule does not apply.</param>
    public RetryClassifier Transient<TException>(Func<TException, bool>? when = null) where TException : Exception
    {
        return With(Rule.For(when, true));
    }

    /// <summary>
    ///     Returns a classifier that treats <typeparamref name="TException" /> as permanent.
    /// </summary>
    /// <param name="when">An optional condition. When it returns false, the rule does not apply.</param>
    public RetryClassifier Permanent<TException>(Func<TException, bool>? when = null) where TException : Exception
    {
        return With(Rule.For(when, false));
    }

    /// <summary>
    ///     Decides whether <paramref name="exception" /> is transient.
    /// </summary>
    /// <param name="exception">The failure.</param>
    /// <param name="pipelineToken">The pipeline's cancellation token. A cancellation it caused is never transient.</param>
    public bool IsTransient(Exception exception, CancellationToken pipelineToken)
    {
        ArgumentNullException.ThrowIfNull(exception);

        if (pipelineToken.IsCancellationRequested)
            return false;

        foreach (var rule in _rules)
        {
            for (var current = exception; current is not null; current = Unwrap(current))
            {
                if (rule.Matches(current))
                    return rule.IsTransient;
            }
        }

        if (_everythingIsTransient)
            return true;

        var root = exception;

        for (var current = exception; current is not null; current = Unwrap(current))
        {
            if (current.Data.Contains(NResilienceAttemptsKey))
                return false;

            root = current;
        }

        return IsTransientByDefault(root);
    }

    private RetryClassifier With(Rule rule)
    {
        return new RetryClassifier(_rules.Add(rule), _everythingIsTransient);
    }

    private static Exception? Unwrap(Exception exception)
    {
        return exception switch
        {
            NodeExecutionException or PipelineExecutionException or TargetInvocationException => exception.InnerException,
            AggregateException { InnerExceptions.Count: 1 } aggregate => aggregate.InnerExceptions[0],
            _ => null,
        };
    }

    private static bool IsTransientByDefault(Exception exception)
    {
        return exception switch
        {
            TimeoutException => true,
            IOException => true,
            SocketException => true,
            HttpRequestException http => http.StatusCode is null || IsTransientStatus(http.StatusCode.Value),
            DbException db => db.IsTransient,

            // The pipeline's own cancellation was ruled out by the caller, so this is a client-side timeout.
            TaskCanceledException => true,
            _ => false,
        };
    }

    private static bool IsTransientStatus(HttpStatusCode status)
    {
        var code = (int)status;
        return code is 408 or 429 or >= 500 and <= 599;
    }

    private readonly record struct Rule(Func<Exception, bool> Matches, bool IsTransient)
    {
        public static Rule For<TException>(Func<TException, bool>? when, bool isTransient) where TException : Exception
        {
            return new Rule(e => e is TException typed && (when is null || when(typed)), isTransient);
        }
    }
}
