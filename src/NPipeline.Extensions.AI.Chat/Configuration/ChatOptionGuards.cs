namespace NPipeline.Extensions.AI.Chat.Configuration;

internal static class ChatOptionGuards
{
    internal static ChatTransformOptions<TIn, TOut> Validate<TIn, TOut>(ChatTransformOptions<TIn, TOut> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.SystemPrompt))
            throw new InvalidOperationException("AI transform options require a non-empty SystemPrompt.");

        if (options.ItemTemplate is null)
            throw new InvalidOperationException("AI transform options require ItemTemplate.");

        return options;
    }

    internal static ChatEnrichmentOptions<TIn, TField> Validate<TIn, TField>(ChatEnrichmentOptions<TIn, TField> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.SystemPrompt))
            throw new InvalidOperationException("AI enrich options require a non-empty SystemPrompt.");

        if (options.ItemTemplate is null)
            throw new InvalidOperationException("AI enrich options require ItemTemplate.");

        if (options.ResultMapper is null)
            throw new InvalidOperationException("AI enrich options require ResultMapper.");

        return options;
    }

    internal static ChatBatchedTransformOptions<TIn, TOut> Validate<TIn, TOut>(ChatBatchedTransformOptions<TIn, TOut> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.SystemPrompt))
            throw new InvalidOperationException("AI batched transform options require a non-empty SystemPrompt.");

        if (options.BatchTemplate is null)
            throw new InvalidOperationException("AI batched transform options require BatchTemplate.");

        return options;
    }

    internal static ChatBatchedEnrichmentOptions<TIn, TField> Validate<TIn, TField>(ChatBatchedEnrichmentOptions<TIn, TField> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.SystemPrompt))
            throw new InvalidOperationException("AI batched enrich options require a non-empty SystemPrompt.");

        if (options.BatchTemplate is null)
            throw new InvalidOperationException("AI batched enrich options require BatchTemplate.");

        if (options.ResultMapper is null)
            throw new InvalidOperationException("AI batched enrich options require ResultMapper.");

        return options;
    }

    internal static ChatBatchedStreamTransformOptions<TIn, TOut> Validate<TIn, TOut>(ChatBatchedStreamTransformOptions<TIn, TOut> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.SystemPrompt))
            throw new InvalidOperationException("AI stream batched transform options require a non-empty SystemPrompt.");

        if (options.BatchTemplate is null)
            throw new InvalidOperationException("AI stream batched transform options require BatchTemplate.");

        if (!options.BatchSize.HasValue || options.BatchSize.Value <= 0)
            throw new InvalidOperationException("AI stream batched transform options require BatchSize > 0.");

        if (options.BatchTimeout.HasValue && options.BatchTimeout.Value <= TimeSpan.Zero)
            throw new InvalidOperationException("AI stream batched transform options require BatchTimeout > 0 when specified.");

        return options;
    }

    internal static ChatBatchedStreamEnrichmentOptions<TIn, TField> Validate<TIn, TField>(ChatBatchedStreamEnrichmentOptions<TIn, TField> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.SystemPrompt))
            throw new InvalidOperationException("AI stream batched enrich options require a non-empty SystemPrompt.");

        if (options.BatchTemplate is null)
            throw new InvalidOperationException("AI stream batched enrich options require BatchTemplate.");

        if (options.ResultMapper is null)
            throw new InvalidOperationException("AI stream batched enrich options require ResultMapper.");

        if (!options.BatchSize.HasValue || options.BatchSize.Value <= 0)
            throw new InvalidOperationException("AI stream batched enrich options require BatchSize > 0.");

        if (options.BatchTimeout.HasValue && options.BatchTimeout.Value <= TimeSpan.Zero)
            throw new InvalidOperationException("AI stream batched enrich options require BatchTimeout > 0 when specified.");

        return options;
    }
}
