namespace NPipeline.Connectors.Messaging;

/// <summary>
///     What a message-queue source sends to the pipeline's dead-letter sink for a message it could not deserialize: the
///     whole body and the broker's properties, so the message can be inspected or published again.
/// </summary>
/// <param name="Source">The topic or queue the message came from.</param>
/// <param name="MessageId">The message's id.</param>
/// <param name="Body">The body as received.</param>
/// <param name="Metadata">The broker's properties and headers.</param>
public sealed record MessageFailure(string Source, string MessageId, ReadOnlyMemory<byte> Body, IReadOnlyDictionary<string, object> Metadata);
