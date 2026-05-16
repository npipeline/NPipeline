using Microsoft.Extensions.AI;

namespace NPipeline.Extensions.AI.Tests;

public sealed class FakeChatClient : IChatClient
{
    private readonly Func<IEnumerable<ChatMessage>, ChatOptions?, CancellationToken, Task<ChatResponse>> _handler;

    public FakeChatClient(Func<IEnumerable<ChatMessage>, ChatOptions?, CancellationToken, Task<ChatResponse>> handler)
    {
        _handler = handler;
    }

    public FakeChatClient(string responseText)
    {
        _handler = (_, _, _) => Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, responseText)));
    }

    public Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages,
        ChatOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        return _handler(messages, options, cancellationToken);
    }

    public IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages,
        ChatOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Streaming not used by NPipeline.Extensions.AI.");
    }

    void IDisposable.Dispose()
    {
    }

    object? IChatClient.GetService(Type serviceType, object? serviceKey)
    {
        return null;
    }

    public static FakeChatClient ThatThrows(Exception exception)
    {
        return new FakeChatClient((_, _, _) => throw exception);
    }

    public static FakeChatClient ThatReturns(string text, string? modelId = null)
    {
        return new FakeChatClient((_, _, _) =>
            Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, text)) { ModelId = modelId }));
    }
}
