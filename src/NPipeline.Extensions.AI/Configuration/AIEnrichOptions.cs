using Microsoft.Extensions.AI;

namespace NPipeline.Extensions.AI.Configuration;

/// <summary>Configuration options for <see cref="Nodes.AIEnrichNode{TIn, TField}" />.</summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TField">The AI-generated field type to splice back into <typeparamref name="TIn" />.</typeparam>
/// <param name="SystemPrompt">The system prompt sent to the LLM. Required.</param>
/// <param name="ItemTemplate">Template delegate that formats each item into the user message. Required.</param>
/// <param name="ResultMapper">Maps the AI-generated field back onto the input item. Required.</param>
/// <param name="Temperature">LLM temperature setting.</param>
/// <param name="MaxOutputTokens">Maximum output tokens.</param>
/// <param name="UseNativeStructuredOutput">When true, requests JSON response format from the model.</param>
/// <param name="ConfigureOptions">Optional callback for advanced <see cref="ChatOptions" /> configuration. Fires last.</param>
public sealed record AIEnrichOptions<TIn, TField>(
    string? SystemPrompt = null,
    Func<TIn, string>? ItemTemplate = null,
    ResultMapper<TIn, TField>? ResultMapper = null,
    float? Temperature = null,
    int? MaxOutputTokens = null,
    bool UseNativeStructuredOutput = false,
    Action<ChatOptions>? ConfigureOptions = null);

/// <summary>Fluent builder for <see cref="AIEnrichOptions{TIn, TField}" />.</summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TField">The AI-generated field type.</typeparam>
public sealed class AIEnrichOptionsBuilder<TIn, TField>
{
    private Action<ChatOptions>? _configureOptions;
    private Func<TIn, string>? _itemTemplate;
    private int? _maxOutputTokens;
    private ResultMapper<TIn, TField>? _resultMapper;
    private string? _systemPrompt;
    private float? _temperature;
    private bool _useNativeStructuredOutput;

    /// <summary>Sets the system prompt.</summary>
    public AIEnrichOptionsBuilder<TIn, TField> WithSystemPrompt(string systemPrompt)
    {
        _systemPrompt = systemPrompt;
        return this;
    }

    /// <summary>Sets the item template delegate.</summary>
    public AIEnrichOptionsBuilder<TIn, TField> WithItemTemplate(Func<TIn, string> itemTemplate)
    {
        _itemTemplate = itemTemplate;
        return this;
    }

    /// <summary>Sets the result mapper delegate that splices the AI field back into the input.</summary>
    public AIEnrichOptionsBuilder<TIn, TField> WithResultMapper(ResultMapper<TIn, TField> resultMapper)
    {
        _resultMapper = resultMapper;
        return this;
    }

    /// <summary>Sets the LLM temperature.</summary>
    public AIEnrichOptionsBuilder<TIn, TField> WithTemperature(float temperature)
    {
        _temperature = temperature;
        return this;
    }

    /// <summary>Sets the maximum output tokens.</summary>
    public AIEnrichOptionsBuilder<TIn, TField> WithMaxOutputTokens(int maxOutputTokens)
    {
        if (maxOutputTokens <= 0)
            throw new ArgumentOutOfRangeException(nameof(maxOutputTokens), "Max output tokens must be positive.");

        _maxOutputTokens = maxOutputTokens;
        return this;
    }

    /// <summary>Enables native structured output (JSON response format).</summary>
    public AIEnrichOptionsBuilder<TIn, TField> WithNativeStructuredOutput(bool useNativeStructuredOutput = true)
    {
        _useNativeStructuredOutput = useNativeStructuredOutput;
        return this;
    }

    /// <summary>Provides an advanced callback for <see cref="ChatOptions" /> configuration.</summary>
    public AIEnrichOptionsBuilder<TIn, TField> WithConfigureOptions(Action<ChatOptions> configureOptions)
    {
        _configureOptions = configureOptions;
        return this;
    }

    /// <summary>Builds the options, validating that required fields are set.</summary>
    public AIEnrichOptions<TIn, TField> Build()
    {
        if (string.IsNullOrWhiteSpace(_systemPrompt))
            throw new InvalidOperationException("SystemPrompt is required.");

        if (_itemTemplate is null)
            throw new InvalidOperationException("ItemTemplate is required.");

        if (_resultMapper is null)
            throw new InvalidOperationException("ResultMapper is required.");

        return new AIEnrichOptions<TIn, TField>(
            _systemPrompt,
            _itemTemplate,
            _resultMapper,
            _temperature,
            _maxOutputTokens,
            _useNativeStructuredOutput,
            _configureOptions);
    }
}
