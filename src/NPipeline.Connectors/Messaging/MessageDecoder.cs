using System.Diagnostics.CodeAnalysis;
using System.Text;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Errors;
using NPipeline.ErrorHandling;

namespace NPipeline.Connectors.Messaging;

/// <summary>
///     Deserializes message bodies for a message-queue source and handles the ones that do not deserialize with the shared
///     <see cref="RowErrorHandler" />. <see cref="TryDecode" /> is the fast path; <see cref="HandleFailureAsync" /> handles a
///     body that failed: <c>Fail</c> throws a <see cref="RecordMappingException" />, <c>Skip</c> and <c>DeadLetter</c> return so
///     the source can settle the message, after sending a <see cref="MessageFailure" /> to the dead-letter sink for
///     <c>DeadLetter</c>.
/// </summary>
/// <typeparam name="T">The body type.</typeparam>
public sealed class MessageDecoder<T>
{
    private readonly MessageContext _context;
    private readonly int _excerptLength;
    private readonly RowErrorHandler? _handler;
    private readonly IMessageSerializer _serializer;

    /// <summary>Creates a decoder for one source.</summary>
    /// <param name="connector">The connector's name in metrics, for example <c>kafka</c>.</param>
    /// <param name="destination">The topic or queue, reported as the row error's source.</param>
    /// <param name="serializer">The body serializer.</param>
    /// <param name="handler">Decides what happens to a body that does not deserialize; <c>null</c> fails the read.</param>
    /// <param name="excerptLength">The most characters of the body kept in <see cref="RowError.RawExcerpt" />.</param>
    public MessageDecoder(string connector, string destination, IMessageSerializer serializer, RowErrorHandler? handler, int excerptLength)
    {
        Connector = connector ?? throw new ArgumentNullException(nameof(connector));
        _context = new MessageContext(destination ?? throw new ArgumentNullException(nameof(destination)));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        _handler = handler;
        _excerptLength = excerptLength;
    }

    /// <summary>The connector's name in metrics.</summary>
    public string Connector { get; }

    /// <summary>Deserializes a body that needs no row-error handling, for example a Kafka key.</summary>
    public TValue Deserialize<TValue>(ReadOnlySpan<byte> bytes, bool isKey) => _serializer.Deserialize<TValue>(bytes, _context with { IsKey = isKey });

    /// <summary>Deserializes <paramref name="body" />; on failure returns <c>false</c> and the error, for <see cref="HandleFailureAsync" />.</summary>
    public bool TryDecode(ReadOnlySpan<byte> body, [MaybeNullWhen(false)] out T value, [NotNullWhen(false)] out Exception? error)
    {
        try
        {
            value = _serializer.Deserialize<T>(body, _context);
            error = null;
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            value = default;
            error = ex;
            return false;
        }
    }

    /// <summary>
    ///     Asks the row-error handler what to do with a body that did not deserialize: throws a
    ///     <see cref="RecordMappingException" /> for <c>Fail</c>, sends a <see cref="MessageFailure" /> to
    ///     <paramref name="deadLetters" /> for <c>DeadLetter</c>, and returns the action for <c>Skip</c> and <c>DeadLetter</c>,
    ///     so the source can settle the message.
    /// </summary>
    /// <param name="error">Why the body did not deserialize.</param>
    /// <param name="body">The body as received.</param>
    /// <param name="messageId">The message's id.</param>
    /// <param name="sequence">The message's 1-based position in this read, or its offset, reported as the row error's record number.</param>
    /// <param name="metadata">The broker's properties, for the dead letter.</param>
    /// <param name="deadLetters">The source's dead-letter channel.</param>
    /// <param name="cancellationToken">Cancels the dead-letter send.</param>
    public async ValueTask<RowErrorAction> HandleFailureAsync(Exception error, ReadOnlyMemory<byte> body, string messageId, long sequence,
        IReadOnlyDictionary<string, object> metadata, DeadLetterChannel deadLetters, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(error);

        var rowError = new RowError(_context.Destination, sequence, null, Excerpt(body.Span, _excerptLength), error);
        var action = _handler?.Invoke(rowError) ?? RowErrorAction.Fail;
        ConnectorDiagnostics.RecordRowError(Connector, Connector, action.ToString().ToLowerInvariant());

        switch (action)
        {
            case RowErrorAction.Skip:
                return action;
            case RowErrorAction.DeadLetter:
                await deadLetters.SendAsync(new MessageFailure(_context.Destination, messageId, body.ToArray(), metadata), error, cancellationToken)
                    .ConfigureAwait(false);

                return action;
            default:
                throw new RecordMappingException(rowError);
        }
    }

    private static string? Excerpt(ReadOnlySpan<byte> body, int length)
    {
        if (length <= 0 || body.IsEmpty)
            return null;

        // Enough bytes for length characters of UTF-8; a character cut in half decodes as a replacement.
        var take = Math.Min(body.Length, length * 4);
        var text = Encoding.UTF8.GetString(body[..take]);

        return text.Length <= length && take == body.Length
            ? text
            : string.Concat(text.AsSpan(0, Math.Min(text.Length, length)), "…");
    }
}
