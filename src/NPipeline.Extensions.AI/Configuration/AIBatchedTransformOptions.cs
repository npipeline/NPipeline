using Microsoft.Extensions.AI;

namespace NPipeline.Extensions.AI.Configuration;

/// <summary>Configuration options for <see cref="Nodes.AIBatchedTransformNode{TIn, TOut}" />.</summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
/// <param name="SystemPrompt">The system prompt sent to the LLM. Required.</param>
/// <param name="BatchTemplate">Template delegate that formats a batch into the user message. Required.</param>
/// <param name="Temperature">LLM temperature setting.</param>
/// <param name="MaxOutputTokens">Maximum output tokens.</param>
/// <param name="UseNativeStructuredOutput">When true, requests JSON response format from the model.</param>
/// <param name="ConfigureOptions">Optional callback for advanced <see cref="ChatOptions" /> configuration. Fires last.</param>
public sealed record AIBatchedTransformOptions<TIn, TOut>(
    string? SystemPrompt = null,
    Func<IReadOnlyCollection<TIn>, string>? BatchTemplate = null,
    float? Temperature = null,
    int? MaxOutputTokens = null,
    bool UseNativeStructuredOutput = false,
    Action<ChatOptions>? ConfigureOptions = null);

/// <summary>Fluent builder for <see cref="AIBatchedTransformOptions{TIn, TOut}" />.</summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
public sealed class AIBatchedTransformOptionsBuilder<TIn, TOut>
{
    private Func<IReadOnlyCollection<TIn>, string>? _batchTemplate;
    private Action<ChatOptions>? _configureOptions;
    private int? _maxOutputTokens;
    private string? _systemPrompt;
    private float? _temperature;
    private bool _useNativeStructuredOutput;

    /// <summary>Sets the system prompt.</summary>
    public AIBatchedTransformOptionsBuilder<TIn, TOut> WithSystemPrompt(string systemPrompt)
    {
        _systemPrompt = systemPrompt;
        return this;
    }

    /// <summary>Sets the batch template delegate.</summary>
    public AIBatchedTransformOptionsBuilder<TIn, TOut> WithBatchTemplate(Func<IReadOnlyCollection<TIn>, string> batchTemplate)
    {
        _batchTemplate = batchTemplate;
        return this;
    }

    /// <summary>Sets the LLM temperature.</summary>
    public AIBatchedTransformOptionsBuilder<TIn, TOut> WithTemperature(float temperature)
    {
        _temperature = temperature;
        return this;
    }

    /// <summary>Sets the maximum output tokens.</summary>
    public AIBatchedTransformOptionsBuilder<TIn, TOut> WithMaxOutputTokens(int maxOutputTokens)
    {
        if (maxOutputTokens <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxOutputTokens), "Max output tokens must be positive.");

        _maxOutputTokens = maxOutputTokens;
        return this;
    }

    /// <summary>Enables native structured output (JSON response format).</summary>
    public AIBatchedTransformOptionsBuilder<TIn, TOut> WithNativeStructuredOutput(bool useNativeStructuredOutput = true)
    {
        _useNativeStructuredOutput = useNativeStructuredOutput;
        return this;
    }

    /// <summary>Provides an advanced callback for <see cref="ChatOptions" /> configuration.</summary>
    public AIBatchedTransformOptionsBuilder<TIn, TOut> WithConfigureOptions(Action<ChatOptions> configureOptions)
    {
        _configureOptions = configureOptions;
        return this;
    }

    /// <summary>Builds the options, validating that required fields are set.</summary>
    public AIBatchedTransformOptions<TIn, TOut> Build()
    {
        if (string.IsNullOrWhiteSpace(_systemPrompt))
            throw new InvalidOperationException("SystemPrompt is required.");

        if (_batchTemplate is null)
            throw new InvalidOperationException("BatchTemplate is required.");

        return new AIBatchedTransformOptions<TIn, TOut>(
            _systemPrompt,
            _batchTemplate,
            _temperature,
            _maxOutputTokens,
            _useNativeStructuredOutput,
            _configureOptions);
    }
}
