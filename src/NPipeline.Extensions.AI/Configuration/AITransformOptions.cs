using Microsoft.Extensions.AI;

namespace NPipeline.Extensions.AI.Configuration;

/// <summary>Configuration options for <see cref="Nodes.AITransformNode{TIn, TOut}" />.</summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
/// <param name="SystemPrompt">The system prompt sent to the LLM. Required.</param>
/// <param name="ItemTemplate">Template delegate that formats each item into the user message. Required.</param>
/// <param name="Temperature">LLM temperature setting.</param>
/// <param name="MaxOutputTokens">Maximum output tokens.</param>
/// <param name="UseNativeStructuredOutput">When true, requests JSON response format from the model.</param>
/// <param name="ConfigureOptions">Optional callback for advanced <see cref="ChatOptions" /> configuration. Fires last.</param>
public sealed record AITransformOptions<TIn, TOut>(
    string? SystemPrompt = null,
    Func<TIn, string>? ItemTemplate = null,
    float? Temperature = null,
    int? MaxOutputTokens = null,
    bool UseNativeStructuredOutput = false,
    Action<ChatOptions>? ConfigureOptions = null);

/// <summary>Fluent builder for <see cref="AITransformOptions{TIn, TOut}" />.</summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
public sealed class AITransformOptionsBuilder<TIn, TOut>
{
    private Action<ChatOptions>? _configureOptions;
    private Func<TIn, string>? _itemTemplate;
    private int? _maxOutputTokens;
    private string? _systemPrompt;
    private float? _temperature;
    private bool _useNativeStructuredOutput;

    /// <summary>Sets the system prompt.</summary>
    public AITransformOptionsBuilder<TIn, TOut> WithSystemPrompt(string systemPrompt)
    {
        _systemPrompt = systemPrompt;
        return this;
    }

    /// <summary>Sets the item template delegate.</summary>
    public AITransformOptionsBuilder<TIn, TOut> WithItemTemplate(Func<TIn, string> itemTemplate)
    {
        _itemTemplate = itemTemplate;
        return this;
    }

    /// <summary>Sets the LLM temperature.</summary>
    public AITransformOptionsBuilder<TIn, TOut> WithTemperature(float temperature)
    {
        _temperature = temperature;
        return this;
    }

    /// <summary>Sets the maximum output tokens.</summary>
    public AITransformOptionsBuilder<TIn, TOut> WithMaxOutputTokens(int maxOutputTokens)
    {
        if (maxOutputTokens <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxOutputTokens), "Max output tokens must be positive.");

        _maxOutputTokens = maxOutputTokens;
        return this;
    }

    /// <summary>Enables native structured output (JSON response format).</summary>
    public AITransformOptionsBuilder<TIn, TOut> WithNativeStructuredOutput(bool useNativeStructuredOutput = true)
    {
        _useNativeStructuredOutput = useNativeStructuredOutput;
        return this;
    }

    /// <summary>Provides an advanced callback for <see cref="ChatOptions" /> configuration.</summary>
    public AITransformOptionsBuilder<TIn, TOut> WithConfigureOptions(Action<ChatOptions> configureOptions)
    {
        _configureOptions = configureOptions;
        return this;
    }

    /// <summary>Builds the options, validating that required fields are set.</summary>
    public AITransformOptions<TIn, TOut> Build()
    {
        if (string.IsNullOrWhiteSpace(_systemPrompt))
            throw new InvalidOperationException("SystemPrompt is required.");

        if (_itemTemplate is null)
            throw new InvalidOperationException("ItemTemplate is required.");

        return new AITransformOptions<TIn, TOut>(
            _systemPrompt,
            _itemTemplate,
            _temperature,
            _maxOutputTokens,
            _useNativeStructuredOutput,
            _configureOptions);
    }
}
