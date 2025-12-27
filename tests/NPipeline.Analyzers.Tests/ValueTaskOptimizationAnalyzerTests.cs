using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Nodes;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for the ValueTaskOptimizationAnalyzer.
/// </summary>
public sealed class ValueTaskOptimizationAnalyzerTests
{
    [Fact]
    public void ShouldDetectTaskFromResultWithoutExecuteValueTaskAsyncOverride()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(item.Length); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should detect Task.FromResult usage without ExecuteValueTaskAsync override");
    }

    [Fact]
    public void ShouldDetectTaskFromResultWithComplexExpressions()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           var result = item?.Trim().Length ?? 0;
                           return Task.FromResult(result * 2); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should detect Task.FromResult usage in complex expressions");
    }

    [Fact]
    public void ShouldDetectTaskFromResultInConditionalExpressions()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, string>
                   {
                       public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return string.IsNullOrEmpty(item) 
                               ? Task.FromResult("empty") // Should trigger diagnostic
                               : Task.FromResult(item.ToUpper()); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should detect Task.FromResult usage in conditional expressions");
    }

    [Fact]
    public void ShouldDetectTaskFromResultInSwitchExpressions()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return item switch
                           {
                               null => Task.FromResult(0), // Should trigger diagnostic
                               "" => Task.FromResult(1), // Should trigger diagnostic
                               _ => Task.FromResult(item.Length) // Should trigger diagnostic
                           };
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should detect Task.FromResult usage in switch expressions");
    }

    [Fact]
    public void ShouldDetectGenericTaskFromResult()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task<int>.FromResult(item.Length); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should detect generic Task.FromResult usage");
    }

    [Fact]
    public void ShouldNotDetectWhenExecuteValueTaskAsyncIsOverridden()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(item.Length); // Should NOT trigger diagnostic
                       }

                       protected internal override ValueTask<int> ExecuteValueTaskAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return ValueTask.FromResult(item.Length);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should not trigger when ExecuteValueTaskAsync is overridden");
    }

    [Fact]
    public void ShouldNotDetectWhenTaskFromResultIsNotUsed()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.Run(() => item.Length); // Should NOT trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should not trigger when Task.FromResult is not used");
    }

    [Fact]
    public void ShouldNotDetectNonTransformNodeClasses()
    {
        var code = """
                   using System.Threading.Tasks;

                   public class TestClass
                   {
                       public Task<int> TestMethod(string item)
                       {
                           return Task.FromResult(item.Length); // Should NOT trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should not trigger for non-TransformNode classes");
    }

    [Fact]
    public void ShouldNotDetectWhenBothExecuteAsyncAndExecuteValueTaskAsyncAreOverridden()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(item.Length); // Should NOT trigger diagnostic
                       }

                       protected internal override ValueTask<int> ExecuteValueTaskAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return ValueTask.FromResult(item.Length);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should not trigger when both ExecuteAsync and ExecuteValueTaskAsync are overridden");
    }

    [Fact]
    public void ShouldNotDetectWhenExecuteAsyncUsesAwait()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override async Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           await Task.Delay(1); // Truly async
                           return item.Length; // Should NOT trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should not trigger when ExecuteAsync uses await");
    }

    [Fact]
    public void ShouldDetectWhenExecuteAsyncHasEmptyBodyButUsesTaskFromResult()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(0); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should detect Task.FromResult usage in empty method body");
    }

    [Fact]
    public void ShouldNotDetectWhenExecuteAsyncReturnsNull()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return null; // Should NOT trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should not trigger when ExecuteAsync returns null");
    }

    [Fact]
    public void ShouldNotDetectWhenExecuteAsyncReturnsCompletedTask()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.CompletedTask; // Should NOT trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should not trigger when ExecuteAsync returns Task.CompletedTask");
    }

    [Fact]
    public void ShouldDetectMultipleTaskFromResultUsages()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           var result1 = Task.FromResult(item.Length); // Should trigger diagnostic
                           var result2 = Task.FromResult(item.Length * 2); // Should trigger diagnostic
                           return result1;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should detect multiple Task.FromResult usages");
    }

    [Fact]
    public void ShouldDetectTaskFromResultInTernaryOperator()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return string.IsNullOrEmpty(item) 
                               ? Task.FromResult(0) // Should trigger diagnostic
                               : Task.FromResult(item.Length); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should detect Task.FromResult usage in ternary operator");
    }

    [Fact]
    public void ShouldDetectTaskFromResultInLambdaExpression()
    {
        var code = """
                   using System;
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           Func<int> lambda = () => item.Length;
                           return Task.FromResult(lambda()); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should detect Task.FromResult usage in lambda expression");
    }

    [Fact]
    public void ShouldDetectTaskFromResultInLocalFunction()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           int LocalFunction()
                           {
                               return item.Length;
                           }
                           return Task.FromResult(LocalFunction()); // Should trigger diagnostic
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.True(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should detect Task.FromResult usage in local function");
    }

    [Fact]
    public void ShouldNotDetectWhenExecuteValueTaskAsyncIsOverriddenButExecuteAsyncStillUsesTaskFromResult()
    {
        var code = """
                   using System.Threading.Tasks;
                   using NPipeline.Pipeline;
                   using NPipeline.Nodes;

                   public class TestTransform : TransformNode<string, int>
                   {
                       public override Task<int> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(item.Length); // Should NOT trigger diagnostic
                       }

                       protected internal override ValueTask<int> ExecuteValueTaskAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return ValueTask.FromResult(item.Length);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        Assert.False(diagnostics.Any(d => d.Id == ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId),
            "Analyzer should not trigger when ExecuteValueTaskAsync is overridden even if ExecuteAsync uses Task.FromResult");
    }

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(ValueTask<>).Assembly.Location),

            // Add reference to NPipeline assembly - use TransformNode instead of Pipeline
            MetadataReference.CreateFromFile(typeof(TransformNode<,>).Assembly.Location),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        var analyzer = new ValueTaskOptimizationAnalyzer();
        var compilationWithAnalyzers = compilation.WithAnalyzers(ImmutableArray.Create<DiagnosticAnalyzer>(analyzer));
        var diagnostics = compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}
