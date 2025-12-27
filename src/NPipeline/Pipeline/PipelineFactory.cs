using NPipeline.Nodes;

namespace NPipeline.Pipeline;

/// <summary>
///     Creates pipelines from definitions.
/// </summary>
public sealed class PipelineFactory : IPipelineFactory
{
    /// <inheritdoc />
    public Pipeline Create<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new()
    {
        ArgumentNullException.ThrowIfNull(context);

        var definition = new TDefinition();

        var builder = new PipelineBuilder();

        definition.Define(builder, context);

        // Allow tests / advanced users to supply preconfigured node instances via context.
        // Context.Items[PipelineContextKeys.PreconfiguredNodes] expected to be Dictionary<string, INode>.
        if (context.Items.TryGetValue(PipelineContextKeys.PreconfiguredNodes, out var preCfg) && preCfg is Dictionary<string, INode> map)
        {
            foreach (var kvp in map)

                // Best-effort: ignore duplicates (will throw) so wrap in try/catch.
            {
                try
                {
                    builder.AddPreconfiguredNodeInstance(kvp.Key, kvp.Value);
                }
                catch
                {
                    /* ignore */
                }
            }
        }

        return builder.Build();
    }
}
