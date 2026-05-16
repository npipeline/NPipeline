using System.Text.Json;
using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Exceptions;

namespace NPipeline.Extensions.AI;

internal static class AIInvoker
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    internal static async Task<TOut> InvokeTransformAsync<TIn, TOut>(
        IChatClient chatClient,
        TIn item,
        string systemPrompt,
        Func<TIn, string> itemTemplate,
        float? temperature,
        int? maxOutputTokens,
        bool useNativeStructuredOutput,
        Action<ChatOptions>? configureOptions,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentException.ThrowIfNullOrWhiteSpace(systemPrompt);
        ArgumentNullException.ThrowIfNull(itemTemplate);

        var userMessage = itemTemplate(item) ?? throw new InvalidOperationException("ItemTemplate returned null.");
        var messages = BuildMessages(systemPrompt, userMessage);
        var options = BuildChatOptions(temperature, maxOutputTokens, useNativeStructuredOutput, configureOptions);

        return await InvokeAndDeserializeAsync<TOut>(chatClient, item, userMessage, messages, options, cancellationToken).ConfigureAwait(false);
    }

    internal static async Task<IReadOnlyCollection<TOut>> InvokeBatchedTransformAsync<TIn, TOut>(
        IChatClient chatClient,
        IReadOnlyCollection<TIn> batch,
        string systemPrompt,
        Func<IReadOnlyCollection<TIn>, string> batchTemplate,
        float? temperature,
        int? maxOutputTokens,
        bool useNativeStructuredOutput,
        Action<ChatOptions>? configureOptions,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentException.ThrowIfNullOrWhiteSpace(systemPrompt);
        ArgumentNullException.ThrowIfNull(batchTemplate);

        var userMessage = batchTemplate(batch) ?? throw new InvalidOperationException("BatchTemplate returned null.");
        var messages = BuildMessages(systemPrompt, userMessage);
        var options = BuildChatOptions(temperature, maxOutputTokens, useNativeStructuredOutput, configureOptions);

        var results = await InvokeAndDeserializeAsync<IReadOnlyCollection<TOut>>(chatClient, batch, userMessage, messages, options, cancellationToken)
            .ConfigureAwait(false);

        if (batch.Count != results.Count)
        {
            throw new AITransformException(
                $"Batch transform count mismatch: sent {batch.Count} items but received {results.Count} results. Expected one result per input item.",
                new InvalidOperationException("Batch transform count mismatch."))
            {
                OriginalItem = batch,
                PromptSent = userMessage,
            };
        }

        return results;
    }

    internal static async Task<TField> InvokeEnrichAsync<TIn, TField>(
        IChatClient chatClient,
        TIn item,
        string systemPrompt,
        Func<TIn, string> itemTemplate,
        float? temperature,
        int? maxOutputTokens,
        bool useNativeStructuredOutput,
        Action<ChatOptions>? configureOptions,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentException.ThrowIfNullOrWhiteSpace(systemPrompt);
        ArgumentNullException.ThrowIfNull(itemTemplate);

        var userMessage = itemTemplate(item) ?? throw new InvalidOperationException("ItemTemplate returned null.");
        var messages = BuildMessages(systemPrompt, userMessage);
        var options = BuildChatOptions(temperature, maxOutputTokens, useNativeStructuredOutput, configureOptions);

        return await InvokeAndDeserializeAsync<TField>(chatClient, item, userMessage, messages, options, cancellationToken).ConfigureAwait(false);
    }

    internal static async Task<IReadOnlyCollection<TField>> InvokeBatchedEnrichAsync<TIn, TField>(
        IChatClient chatClient,
        IReadOnlyCollection<TIn> batch,
        string systemPrompt,
        Func<IReadOnlyCollection<TIn>, string> batchTemplate,
        float? temperature,
        int? maxOutputTokens,
        bool useNativeStructuredOutput,
        Action<ChatOptions>? configureOptions,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentException.ThrowIfNullOrWhiteSpace(systemPrompt);
        ArgumentNullException.ThrowIfNull(batchTemplate);

        var userMessage = batchTemplate(batch) ?? throw new InvalidOperationException("BatchTemplate returned null.");
        var messages = BuildMessages(systemPrompt, userMessage);
        var options = BuildChatOptions(temperature, maxOutputTokens, useNativeStructuredOutput, configureOptions);

        var results = await InvokeAndDeserializeAsync<IReadOnlyCollection<TField>>(chatClient, batch, userMessage, messages, options, cancellationToken)
            .ConfigureAwait(false);

        if (batch.Count != results.Count)
        {
            throw new AITransformException(
                $"Batch enrichment count mismatch: sent {batch.Count} items but received {results.Count} results. Expected one result per input item.",
                new InvalidOperationException("Batch enrichment count mismatch."))
            {
                OriginalItem = batch,
                PromptSent = userMessage,
            };
        }

        return results;
    }

    private static async Task<T> InvokeAndDeserializeAsync<T>(
        IChatClient chatClient,
        object? item,
        string userMessage,
        IEnumerable<ChatMessage> messages,
        ChatOptions options,
        CancellationToken cancellationToken)
    {
        ChatResponse response;

        try
        {
            response = await chatClient.GetResponseAsync(messages, options, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            if (IsInfrastructureException(ex))
                throw;

            throw WrapException(item, userMessage, ex, null);
        }

        var rawText = response.Text;
        var modelId = response.ModelId;

        if (string.IsNullOrWhiteSpace(rawText))
        {
            throw new AITransformException("LLM returned a null or empty response.",
                new InvalidOperationException("ChatResponse.Text was null, empty, or whitespace."))
            {
                OriginalItem = item,
                PromptSent = userMessage,
                ModelUsed = modelId,
            };
        }

        try
        {
            var result = JsonSerializer.Deserialize<T>(rawText, JsonOptions);

            if (result is null)
            {
                throw new AITransformException("LLM returned a JSON null value. Expected a typed object.",
                    new InvalidOperationException("Deserialization produced null."))
                {
                    OriginalItem = item,
                    PromptSent = userMessage,
                    RawResponse = rawText,
                    ModelUsed = modelId,
                };
            }

            return result;
        }
        catch (JsonException ex)
        {
            throw new AITransformException("Failed to deserialize LLM response.", ex)
            {
                OriginalItem = item,
                PromptSent = userMessage,
                RawResponse = rawText,
                ModelUsed = modelId,
            };
        }
    }

    private static bool IsInfrastructureException(Exception ex)
    {
        return ex is OperationCanceledException
               || ex is HttpRequestException
               || ex is TimeoutException;
    }

    private static List<ChatMessage> BuildMessages(string systemPrompt, string userMessage)
    {
        return
        [
            new ChatMessage(ChatRole.System, systemPrompt),
            new ChatMessage(ChatRole.User, userMessage),
        ];
    }

    private static ChatOptions BuildChatOptions(
        float? temperature,
        int? maxOutputTokens,
        bool useNativeStructuredOutput,
        Action<ChatOptions>? configureOptions)
    {
        var options = new ChatOptions();

        if (temperature.HasValue)
            options.Temperature = temperature.Value;

        if (maxOutputTokens.HasValue)
            options.MaxOutputTokens = maxOutputTokens.Value;

        if (useNativeStructuredOutput)
            options.ResponseFormat = ChatResponseFormat.Json;

        configureOptions?.Invoke(options);

        return options;
    }

    private static AITransformException WrapException(object? item, string userMessage, Exception inner, string? modelId)
    {
        return new AITransformException("AI transform failed.", inner)
        {
            OriginalItem = item,
            PromptSent = userMessage,
            ModelUsed = modelId,
        };
    }
}
