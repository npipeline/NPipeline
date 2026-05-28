using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Exceptions;

namespace NPipeline.Extensions.AI;

internal static class AIInvoker
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
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

        var userMessage = BuildUserMessage(item, itemTemplate, "ItemTemplate");
        var messages = BuildMessages(systemPrompt, userMessage);
        var options = BuildChatOptions(item, userMessage, temperature, maxOutputTokens, useNativeStructuredOutput, configureOptions);

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

        var userMessage = BuildUserMessage(batch, batchTemplate, "BatchTemplate");
        var messages = BuildMessages(systemPrompt, userMessage);
        var options = BuildBatchChatOptions<TOut>(batch, userMessage, temperature, maxOutputTokens, useNativeStructuredOutput, configureOptions);

        var wrapper = await InvokeAndDeserializeAsync<BatchResponseWrapper<TOut>>(chatClient, batch, userMessage, messages, options, cancellationToken)
            .ConfigureAwait(false);
        var results = (IReadOnlyCollection<TOut>)wrapper.Items;

        return await RetryBatchOnCountMismatchAsync(
            chatClient, batch, batch.Count, results, systemPrompt, userMessage, "transform", options, cancellationToken)
            .ConfigureAwait(false);
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

        var userMessage = BuildUserMessage(item, itemTemplate, "ItemTemplate");
        var messages = BuildMessages(systemPrompt, userMessage);
        var options = BuildChatOptions(item, userMessage, temperature, maxOutputTokens, useNativeStructuredOutput, configureOptions);

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

        var userMessage = BuildUserMessage(batch, batchTemplate, "BatchTemplate");
        var messages = BuildMessages(systemPrompt, userMessage);
        var options = BuildBatchChatOptions<TField>(batch, userMessage, temperature, maxOutputTokens, useNativeStructuredOutput, configureOptions);

        var wrapper = await InvokeAndDeserializeAsync<BatchResponseWrapper<TField>>(chatClient, batch, userMessage, messages, options, cancellationToken)
            .ConfigureAwait(false);
        var results = (IReadOnlyCollection<TField>)wrapper.Items;

        return await RetryBatchOnCountMismatchAsync(
            chatClient, batch, batch.Count, results, systemPrompt, userMessage, "enrichment", options, cancellationToken)
            .ConfigureAwait(false);
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
            if (ex is AITransformException)
                throw;

            if (IsInfrastructureException(ex))
                throw;

            throw WrapException(item, userMessage, ex, null);
        }

        var rawResponse = response.Text;
        var rawText = SanitizeLlmResponse(rawResponse);
        var modelId = response.ModelId;

        if (string.IsNullOrWhiteSpace(rawText))
        {
            throw new AITransformException("LLM returned a null or empty response.",
                new InvalidOperationException("ChatResponse.Text was null, empty, or whitespace."))
            {
                OriginalItem = item,
                PromptSent = userMessage,
                RawResponse = rawResponse,
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
                    RawResponse = rawResponse,
                    ModelUsed = modelId,
                };
            }

            return result;
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException)
        {
            // Model returned a bare array [...] when a BatchResponseWrapper was expected.
            if (typeof(T).IsGenericType
                && typeof(T).GetGenericTypeDefinition() == typeof(BatchResponseWrapper<>)
                && rawText.StartsWith("[", StringComparison.Ordinal))
            {
                try
                {
                    var wrappedWrapper = $"{{\"Items\":{rawText}}}";
                    var retryResult = JsonSerializer.Deserialize<T>(wrappedWrapper, JsonOptions);

                    if (retryResult is not null)
                        return retryResult;
                }
                catch (Exception retryEx) when (retryEx is JsonException or NotSupportedException)
                {
                    // Wrapping did not help; fall through to the original error.
                }
            }

            // Some models return a single object {…} when the caller expects an array […].
            // If T is IReadOnlyCollection<TField>, wrap in [...] and retry.
            if (IsReadOnlyCollectionType(typeof(T))
                && rawText.StartsWith("{", StringComparison.Ordinal))
            {
                try
                {
                    var wrapped = $"[{rawText}]";
                    var retryResult = JsonSerializer.Deserialize<T>(wrapped, JsonOptions);

                    if (retryResult is not null)
                        return retryResult;
                }
                catch (Exception retryEx) when (retryEx is JsonException or NotSupportedException)
                {
                    // Wrapping did not help; fall through to the original error.
                }
            }

            throw new AITransformException("Failed to deserialize LLM response.", ex)
            {
                OriginalItem = item,
                PromptSent = userMessage,
                RawResponse = rawResponse,
                ModelUsed = modelId,
            };
        }
    }

    private static async Task<IReadOnlyCollection<T>> RetryBatchOnCountMismatchAsync<T>(
        IChatClient chatClient,
        object batch,
        int expectedCount,
        IReadOnlyCollection<T> results,
        string systemPrompt,
        string originalUserMessage,
        string operationName,
        ChatOptions options,
        CancellationToken cancellationToken)
    {
        if (expectedCount == results.Count)
            return results;

        var retryUserMessage =
            $"{originalUserMessage}\n\nIMPORTANT: You provided {results.Count} result(s) but there are {expectedCount} item(s) in the previous user message. Return a JSON object with an 'Items' array containing EXACTLY {expectedCount} elements.";
        var retryMessages = BuildMessages(systemPrompt, retryUserMessage);

        var retryWrapper = await InvokeAndDeserializeAsync<BatchResponseWrapper<T>>(
                chatClient,
                batch,
                retryUserMessage,
                retryMessages,
                options,
                cancellationToken)
            .ConfigureAwait(false);
        results = (IReadOnlyCollection<T>)retryWrapper.Items;

        if (expectedCount != results.Count)
        {
            throw new AITransformException(
                $"Batch {operationName} count mismatch: sent {expectedCount} items but received {results.Count} results after retry. Expected one result per input item.",
                new InvalidOperationException($"Batch {operationName} count mismatch."))
            {
                OriginalItem = batch,
                PromptSent = originalUserMessage,
            };
        }

        return results;
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
        object? item,
        string userMessage,
        float? temperature,
        int? maxOutputTokens,
        bool useNativeStructuredOutput,
        Action<ChatOptions>? configureOptions)
    {
        return BuildChatOptionsCore(
            item,
            userMessage,
            temperature,
            maxOutputTokens,
            useNativeStructuredOutput,
            ChatResponseFormat.Json,
            configureOptions);
    }

    /// <summary>
    ///     Builds <see cref="ChatOptions" /> for batched invocations where the expected response is a JSON object with an Items array.
    /// </summary>
    private static ChatOptions BuildBatchChatOptions<TElement>(
        object? item,
        string userMessage,
        float? temperature,
        int? maxOutputTokens,
        bool useNativeStructuredOutput,
        Action<ChatOptions>? configureOptions)
    {
        return BuildChatOptionsCore(
            item,
            userMessage,
            temperature,
            maxOutputTokens,
            useNativeStructuredOutput,
            ChatResponseFormat.ForJsonSchema<BatchResponseWrapper<TElement>>(),
            configureOptions);
    }

    private sealed record BatchResponseWrapper<T>(List<T> Items);

    private static ChatOptions BuildChatOptionsCore(
        object? item,
        string userMessage,
        float? temperature,
        int? maxOutputTokens,
        bool useNativeStructuredOutput,
        ChatResponseFormat nativeStructuredFormat,
        Action<ChatOptions>? configureOptions)
    {
        var options = new ChatOptions();

        if (temperature.HasValue)
            options.Temperature = temperature.Value;

        if (maxOutputTokens.HasValue)
            options.MaxOutputTokens = maxOutputTokens.Value;

        if (useNativeStructuredOutput)
            options.ResponseFormat = nativeStructuredFormat;

        if (configureOptions is not null)
        {
            try
            {
                configureOptions(options);
            }
            catch (Exception ex)
            {
                if (ex is AITransformException)
                    throw;

                throw new AITransformException("ConfigureOptions delegate failed.", ex)
                {
                    OriginalItem = item,
                    PromptSent = userMessage,
                };
            }
        }

        return options;
    }

    private static string BuildUserMessage<T>(T item, Func<T, string> template, string templateName)
    {
        try
        {
            var userMessage = template(item);

            if (string.IsNullOrWhiteSpace(userMessage))
            {
                throw new AITransformException(
                    $"{templateName} returned null or whitespace.",
                    new InvalidOperationException($"{templateName} returned null or whitespace."))
                {
                    OriginalItem = item,
                };
            }

            return userMessage;
        }
        catch (Exception ex)
        {
            if (ex is AITransformException)
                throw;

            throw new AITransformException($"{templateName} delegate failed.", ex)
            {
                OriginalItem = item,
            };
        }
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

    private static bool IsReadOnlyCollectionType(Type type)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IReadOnlyCollection<>))
            return true;

        foreach (var iface in type.GetInterfaces())
        {
            if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IReadOnlyCollection<>))
                return true;
        }

        return false;
    }

    /// <summary>
    ///     Sanitizes raw LLM response text by stripping markdown code fences before JSON deserialization.
    /// </summary>
    private static string SanitizeLlmResponse(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return string.Empty;

        var trimmed = text.Trim();

        if (trimmed.Length < 6
            || !trimmed.StartsWith("```", StringComparison.Ordinal)
            || !trimmed.EndsWith("```", StringComparison.Ordinal))
            return trimmed;

        var inner = trimmed[3..^3].Trim();

        if (inner.Length == 0)
            return string.Empty;

        // Handles both styles:
        // ```json\n{...}\n```
        // ```json{...}```
        if (inner.StartsWith("json", StringComparison.OrdinalIgnoreCase))
        {
            if (inner.Length == 4)
                return string.Empty;

            var next = inner[4];

            if (char.IsWhiteSpace(next) || next == '{' || next == '[')
                inner = inner[4..].TrimStart();
        }

        return inner.Trim();
    }
}
