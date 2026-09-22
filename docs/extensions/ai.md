---
title: "AI"
description: "Choose an NPipeline AI package for chat transformations, provider-neutral decisions, or TypeSafe AI Jev routing."
order: 11
---

NPipeline separates AI features by capability and provider boundary. Install only the package that owns the behavior you need.

This page helps you choose a package and migrate pre-release APIs. The linked package pages contain the complete configuration, execution, error-handling, and performance references.

## Choose a package

| Package | Capability | Main dependency |
| --- | --- | --- |
| `NPipeline.Extensions.AI` | Shared metadata, usage, and exception contracts | `NPipeline` |
| `NPipeline.Extensions.AI.Chat` | Chat-model transformations, enrichment, and batching | `Microsoft.Extensions.AI.IChatClient` |
| `NPipeline.Extensions.AI.Decisions` | Provider-neutral typed classification and routing | `NPipeline.Extensions.AI` |
| `NPipeline.Extensions.AI.Decisions.Jev` | TypeSafe AI System One client and Jev Choice routing | `NPipeline.Extensions.AI.Decisions` |

Most applications install Chat, Decisions, or Decisions.Jev. NuGet resolves the shared foundation transitively.

## Understand the boundaries

Use Chat when a generative model must create structured output, rewrite data, summarize text, or enrich an item. These APIs use the common `IChatClient` abstraction.

Use Decisions when a model or service returns a typed label, confidence, and probability distribution. Routing retains the original item and doesn't require temporary AI fields in your domain model.

Use Decisions.Jev for near-real-time judgments through TypeSafe AI's Jev model. Jev uses its native System One contract instead of pretending to be a chat model.

## Migrate pre-release APIs

The pre-release chat APIs moved from `NPipeline.Extensions.AI` to `NPipeline.Extensions.AI.Chat`. Their names describe the transport explicitly:

- Replace `AddAITransform` with `AddChatTransform`.
- Replace `AddAIEnrich` with `AddChatEnrichment`.
- Apply the same `Chat` naming to batched and stream-batched APIs.
- Replace `AITransformException` with `ChatTransformException`.

The old mutation-based AI route API isn't available. Implement `IAIClassifier<TInput, TLabel>` and use `AddAIRoute`, or install the Jev provider and use `AddJevRoute`.

## Learn more

- [Chat transformations and enrichment](ai-chat.md)
- [Typed AI decisions and routing](ai-decisions.md)
- [TypeSafe AI Jev decisions](ai-decisions-jev.md)
