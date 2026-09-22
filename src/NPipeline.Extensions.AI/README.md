# NPipeline.Extensions.AI

Shared invocation metadata and exception contracts for NPipeline AI extensions.

Most applications install a feature package instead of referencing this package directly:

| Package                                 | Use it for                                            |
|-----------------------------------------|-------------------------------------------------------|
| `NPipeline.Extensions.AI.Chat`          | Transforming or enriching data with any `IChatClient` |
| `NPipeline.Extensions.AI.Decisions`     | Routing data with a provider-neutral typed classifier |
| `NPipeline.Extensions.AI.Decisions.Jev` | Making typed decisions with TypeSafe AI's Jev model   |

The package defines `AIInvocationMetadata`, `AIUsage`, and `AIInvocationException`. Provider packages use these contracts to report model, request, and token
information consistently.

## Migration

The pre-release chat APIs moved to `NPipeline.Extensions.AI.Chat` and use `Chat` in their names. For example, replace `AddAITransform` with `AddChatTransform`
and `AddAIEnrich` with `AddChatEnrichment`.

AI routing uses `NPipeline.Extensions.AI.Decisions`. Route classifications travel in an internal envelope, so your domain type doesn't need temporary
classification properties.

## License

This package is licensed under the [Business Source License 1.1](LICENSE.txt).

Production use is free for organizations with four or fewer developers and annual revenue of AUD 5 million or less. Larger organizations require
a [commercial license](https://npipeline.com). Each release converts to the MIT License on its change date.
