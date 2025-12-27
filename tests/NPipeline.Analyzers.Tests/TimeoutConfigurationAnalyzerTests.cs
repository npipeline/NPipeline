using NPipeline.CodeFixes;
using VerifyCS = NPipeline.Analyzers.Tests.CSharpCodeFixVerifier;

namespace NPipeline.Analyzers.Tests;

public class TimeoutConfigurationAnalyzerTests
{
    [Fact]
    public async Task Analyzer_ShouldNotReportDiagnostics_WhenCodeIsValid()
    {
        var test = @"
using NPipeline;

class TestTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        // Valid I/O timeout
        builder.AddTransform<IoBoundTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: TimeSpan.FromSeconds(1)));

        // Valid CPU timeout
        builder.AddTransform<CpuBoundTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: TimeSpan.FromMinutes(1)));

        // Note: PipelineRetryOptions doesn't have a Timeout parameter
        // Tests for retry timeout configuration moved to ResilientExecutionStrategy tests
    }
}

class IoBoundTransform { }
class CpuBoundTransform { }
class Input { }
class Output { }
";

        await VerifyCS.VerifyAnalyzerAsync<TimeoutConfigurationAnalyzer>(test);
    }

    [Fact]
    public async Task Analyzer_ShouldReportDiagnostic_WhenIoTimeoutIsTooShort()
    {
        var test = @"
using NPipeline;

class IoBoundTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        // Too short timeout for I/O operations
        builder.AddTransform<IoBoundTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: [|TimeSpan.FromMilliseconds(100)|]));
    }
}

class Input { }
class Output { }
";

        var expectedDiagnostic = VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
            .WithLocation(12, 24)
            .WithArguments(
                "timeout: TimeSpan.FromMilliseconds(100)",
                "iobound",
                "For I/O-bound operations, timeouts should be at least 500ms to account for network/database latency.");

        await VerifyCS.VerifyAnalyzerAsync<TimeoutConfigurationAnalyzer>(test, expectedDiagnostic);
    }

    [Fact]
    public async Task Analyzer_ShouldReportDiagnostic_WhenCpuTimeoutIsTooLong()
    {
        var test = @"
using NPipeline;

class CpuBoundTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        // Too long timeout for CPU operations
        builder.AddTransform<CpuBoundTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: [|TimeSpan.FromMinutes(10)|]));
    }
}

class Input { }
class Output { }
";

        var expectedDiagnostic = VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
            .WithLocation(12, 24)
            .WithArguments(
                "timeout: TimeSpan.FromMinutes(10)",
                "cpubound",
                "For CPU-bound operations, timeouts should not exceed 5 minutes to prevent resource leaks.");

        await VerifyCS.VerifyAnalyzerAsync<TimeoutConfigurationAnalyzer>(test, expectedDiagnostic);
    }

    [Fact]
    public async Task Analyzer_ShouldReportDiagnostic_WhenTimeoutIsZero()
    {
        var test = @"
using NPipeline;

class TestTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        // Zero timeout causes immediate failures
        builder.AddTransform<TestTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: [|TimeSpan.Zero|]));
    }
}

class Input { }
class Output { }
";

        var expectedDiagnostic = VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
            .WithLocation(12, 24)
            .WithArguments(
                "timeout: TimeSpan.Zero",
                "cpubound",
                "Zero or negative timeouts cause immediate failures. Use a positive timeout value.");

        await VerifyCS.VerifyAnalyzerAsync<TimeoutConfigurationAnalyzer>(test, expectedDiagnostic);
    }

    // Note: Test removed - PipelineRetryOptions doesn't have a Timeout parameter
    // Retry timeout configuration is handled at the ResilientExecutionStrategy level

    [Fact]
    public async Task Analyzer_ShouldReportDiagnostic_WhenTimeoutIsNegative()
    {
        var test = @"
using NPipeline;

class TestTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        // Negative timeout
        builder.AddTransform<TestTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: [|TimeSpan.FromMilliseconds(-100)|]));
    }
}

class Input { }
class Output { }
";

        var expectedDiagnostic = VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
            .WithLocation(12, 24)
            .WithArguments(
                "timeout: TimeSpan.FromMilliseconds(-100)",
                "cpubound",
                "Zero or negative timeouts cause immediate failures. Use a positive timeout value.");

        await VerifyCS.VerifyAnalyzerAsync<TimeoutConfigurationAnalyzer>(test, expectedDiagnostic);
    }

    [Fact]
    public async Task Analyzer_ShouldDetectIoBoundWorkload_ByClassName()
    {
        var test = @"
using NPipeline;

class DatabaseTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        // Too short timeout for database operations
        builder.AddTransform<DatabaseTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: [|TimeSpan.FromMilliseconds(200)|]));
    }
}

class Input { }
class Output { }
";

        var expectedDiagnostic = VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
            .WithLocation(12, 24)
            .WithArguments(
                "timeout: TimeSpan.FromMilliseconds(200)",
                "iobound",
                "For I/O-bound operations, timeouts should be at least 500ms to account for network/database latency.");

        await VerifyCS.VerifyAnalyzerAsync<TimeoutConfigurationAnalyzer>(test, expectedDiagnostic);
    }

    [Fact]
    public async Task Analyzer_ShouldDetectCpuBoundWorkload_ByClassName()
    {
        var test = @"
using NPipeline;

class ComputeTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        // Too long timeout for compute operations
        builder.AddTransform<ComputeTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: [|TimeSpan.FromMinutes(8)|]));
    }
}

class Input { }
class Output { }
";

        var expectedDiagnostic = VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
            .WithLocation(12, 24)
            .WithArguments(
                "timeout: TimeSpan.FromMinutes(8)",
                "cpubound",
                "For CPU-bound operations, timeouts should not exceed 5 minutes to prevent resource leaks.");

        await VerifyCS.VerifyAnalyzerAsync<TimeoutConfigurationAnalyzer>(test, expectedDiagnostic);
    }

    [Fact]
    public async Task Analyzer_ShouldDetectTimeoutIssues_WithConstructorArguments()
    {
        var test = @"
using NPipeline;

class TestTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        // Zero timeout as constructor argument
        builder.AddTransform<TestTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                [|TimeSpan.Zero|]));
    }
}

class Input { }
class Output { }
";

        var expectedDiagnostic = VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
            .WithLocation(12, 24)
            .WithArguments(
                "timeout: TimeSpan.Zero",
                "cpubound",
                "Zero or negative timeouts cause immediate failures. Use a positive timeout value.");

        await VerifyCS.VerifyAnalyzerAsync<TimeoutConfigurationAnalyzer>(test, expectedDiagnostic);
    }

    [Fact]
    public async Task Analyzer_ShouldDetectTimeoutIssues_WithNamedArguments()
    {
        var test = @"
using NPipeline;

class TestTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        // Too short timeout with named argument
        builder.AddTransform<TestTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                timeout: [|TimeSpan.FromMilliseconds(50)|]));
    }
}

class Input { }
class Output { }
";

        var expectedDiagnostic = VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
            .WithLocation(12, 32)
            .WithArguments(
                "timeout: TimeSpan.FromMilliseconds(50)",
                "cpubound",
                "For CPU-bound operations, timeouts should be at least 500ms to allow compute-intensive steps to complete.");

        await VerifyCS.VerifyAnalyzerAsync<TimeoutConfigurationAnalyzer>(test, expectedDiagnostic);
    }

    [Fact]
    public async Task Analyzer_ShouldProvideCodeFix_ForIoTimeout()
    {
        var test = @"
using NPipeline;

class IoBoundTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        builder.AddTransform<IoBoundTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: TimeSpan.FromMilliseconds(100)));
    }
}

class Input { }
class Output { }
";

        var fixtest = @"
using NPipeline;

class IoBoundTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        builder.AddTransform<IoBoundTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: TimeSpan.FromMilliseconds(500)));
    }
}

class Input { }
class Output { }
";

        var expectedDiagnostic = VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
            .WithLocation(12, 24)
            .WithArguments(
                "timeout: TimeSpan.FromMilliseconds(100)",
                "iobound",
                "For I/O-bound operations, timeouts should be at least 500ms to account for network/database latency.");

        await VerifyCS.VerifyCodeFixAsync<TimeoutConfigurationAnalyzer, TimeoutConfigurationCodeFixProvider>(test, expectedDiagnostic, fixtest);
    }

    [Fact]
    public async Task Analyzer_ShouldProvideCodeFix_ForCpuTimeout()
    {
        var test = @"
using NPipeline;

class CpuBoundTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        builder.AddTransform<CpuBoundTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: TimeSpan.FromMinutes(10)));
    }
}

class Input { }
class Output { }
";

        var fixtest = @"
using NPipeline;

class CpuBoundTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        builder.AddTransform<CpuBoundTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: TimeSpan.FromMinutes(5)));
    }
}

class Input { }
class Output { }
";

        var expectedDiagnostic = VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
            .WithLocation(12, 24)
            .WithArguments(
                "timeout: TimeSpan.FromMinutes(10)",
                "cpubound",
                "For CPU-bound operations, timeouts should not exceed 5 minutes to prevent resource leaks.");

        await VerifyCS.VerifyCodeFixAsync<TimeoutConfigurationAnalyzer, TimeoutConfigurationCodeFixProvider>(test, expectedDiagnostic, fixtest);
    }

    [Fact]
    public async Task Analyzer_ShouldProvideCodeFix_ForZeroTimeout()
    {
        var test = @"
using NPipeline;

class TestTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        builder.AddTransform<TestTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: TimeSpan.Zero));
    }
}

class Input { }
class Output { }
";

        var fixtest = @"
using NPipeline;

class TestTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        builder.AddTransform<TestTransform, Input, Output>(""transform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: TimeSpan.FromMinutes(1)));
    }
}

class Input { }
class Output { }
";

        var expectedDiagnostic = VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
            .WithLocation(12, 24)
            .WithArguments(
                "timeout: TimeSpan.Zero",
                "cpubound",
                "Zero or negative timeouts cause immediate failures. Use a positive timeout value.");

        await VerifyCS.VerifyCodeFixAsync<TimeoutConfigurationAnalyzer, TimeoutConfigurationCodeFixProvider>(test, expectedDiagnostic, fixtest);
    }

    // Note: Test removed - PipelineRetryOptions doesn't have a Timeout parameter
    // Retry timeout configuration is handled at the ResilientExecutionStrategy level

    [Fact]
    public async Task Analyzer_ShouldHandleMultipleTimeoutIssues()
    {
        var test = @"
using NPipeline;

class TestTransform
{
    public void Configure()
    {
        var builder = new PipelineBuilder();

        // Multiple timeout issues
        builder.AddTransform<IoBoundTransform, Input, Output>(""ioTransform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: [|TimeSpan.FromMilliseconds(50)|]));

        builder.AddTransform<CpuBoundTransform, Input, Output>(""cpuTransform"")
            .WithExecutionStrategy(builder, new ResilientExecutionStrategy(
                Timeout: [|TimeSpan.FromMinutes(15)|]));
    }
}

class IoBoundTransform { }
class CpuBoundTransform { }
class Input { }
class Output { }
";

        var expectedDiagnostics = new[]
        {
            VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
                .WithLocation(13, 24)
                .WithArguments(
                    "timeout: TimeSpan.FromMilliseconds(50)",
                    "iobound",
                    "For I/O-bound operations, timeouts should be at least 500ms to account for network/database latency."),

            VerifyCS.Diagnostic(TimeoutConfigurationAnalyzer.TimeoutConfigurationId)
                .WithLocation(17, 24)
                .WithArguments(
                    "timeout: TimeSpan.FromMinutes(15)",
                    "cpubound",
                    "For CPU-bound operations, timeouts should not exceed 5 minutes to prevent resource leaks."),
        };

        await VerifyCS.VerifyAnalyzerAsync<TimeoutConfigurationAnalyzer>(test, expectedDiagnostics);
    }
}
