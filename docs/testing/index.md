---
title: Testing
description: Test pipelines and individual nodes with in-memory harnesses and assertion helpers.
order: 9
---

# Testing

NPipeline ships a dedicated testing extension with in-memory nodes, a pipeline test harness, and assertion libraries for both AwesomeAssertions and FluentAssertions.

```bash
dotnet add package NPipeline.Extensions.Testing
dotnet add package NPipeline.Extensions.Testing.AwesomeAssertions  # or FluentAssertions
```

**Unit test** individual nodes by calling `TransformAsync` directly. **Integration test** full pipelines using `PipelineTestHarness<T>`, which runs the pipeline, captures results, and provides assertion helpers.

## In This Section

- [Testing Pipelines](testing-pipelines.md) - test harness, in-memory nodes, and integration testing patterns
- [Test Utilities](test-utilities.md) - reference for all test helpers, mocks, and assertion extensions
