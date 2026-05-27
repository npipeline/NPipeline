namespace NPipeline.Extensions.AI.Configuration;

internal static class AIOptionGuards
{
    internal static AITransformOptions<TIn, TOut> Validate<TIn, TOut>(AITransformOptions<TIn, TOut> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.SystemPrompt))
            throw new InvalidOperationException("AI transform options require a non-empty SystemPrompt.");

        if (options.ItemTemplate is null)
            throw new InvalidOperationException("AI transform options require ItemTemplate.");

        return options;
    }

    internal static AIEnrichOptions<TIn, TField> Validate<TIn, TField>(AIEnrichOptions<TIn, TField> options)
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

    internal static AIBatchedTransformOptions<TIn, TOut> Validate<TIn, TOut>(AIBatchedTransformOptions<TIn, TOut> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (string.IsNullOrWhiteSpace(options.SystemPrompt))
            throw new InvalidOperationException("AI batched transform options require a non-empty SystemPrompt.");

        if (options.BatchTemplate is null)
            throw new InvalidOperationException("AI batched transform options require BatchTemplate.");

        return options;
    }

    internal static AIBatchedEnrichOptions<TIn, TField> Validate<TIn, TField>(AIBatchedEnrichOptions<TIn, TField> options)
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

    internal static AIBatchedStreamTransformOptions<TIn, TOut> Validate<TIn, TOut>(AIBatchedStreamTransformOptions<TIn, TOut> options)
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

    internal static AIBatchedStreamEnrichOptions<TIn, TField> Validate<TIn, TField>(AIBatchedStreamEnrichOptions<TIn, TField> options)
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
