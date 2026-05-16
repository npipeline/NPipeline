namespace NPipeline.Extensions.AI.Configuration;

/// <summary>Maps an AI-generated field result back onto an input item during enrichment.</summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TField">The AI-generated field type.</typeparam>
/// <param name="input">The original input item.</param>
/// <param name="aiResult">The AI-generated result to splice in.</param>
/// <returns>The enriched item.</returns>
public delegate TIn ResultMapper<TIn, TField>(TIn input, TField aiResult);
