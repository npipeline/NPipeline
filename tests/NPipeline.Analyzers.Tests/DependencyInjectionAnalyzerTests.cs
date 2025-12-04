using System.Collections.Immutable;
using System.Diagnostics;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Nodes;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Tests for DependencyInjectionAnalyzer.
/// </summary>
public sealed class DependencyInjectionAnalyzerTests
{
    [Fact]
    public void ShouldDetectDirectServiceInstantiationInTransformNode()
    {
        var code = """
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public class BadService
                   {
                       public string Process(string input) => input.ToUpper();
                   }

                   public class BadTransformNode : TransformNode<string, string>
                   {
                       private readonly BadService _badService = new BadService(); // Should trigger diagnostic

                       public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(_badService.Process(item));
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // Debug: Print all diagnostics
        foreach (var diagnostic in diagnostics)
        {
            Debug.WriteLine($"Diagnostic: {diagnostic.Id} - {diagnostic.GetMessage()}");
        }

        var hasDiagnostic = diagnostics.Any(d => d.Id == DependencyInjectionAnalyzer.DependencyInjectionAntiPatternId);
        Assert.True(hasDiagnostic, "Analyzer should detect direct service instantiation in TransformNode");
    }

    [Fact]
    public void DebugTest()
    {
        var code = """
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public class BadService
                   {
                       public string Process(string input) => input.ToUpper();
                   }

                   public class BadTransformNode : TransformNode<string, string>
                   {
                       private readonly BadService _badService = new BadService(); // Should trigger diagnostic

                       public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(_badService.Process(item));
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        // Just print count for now
        Assert.True(diagnostics.Count() >= 0, "This should always pass");
    }

    [Fact]
    public void ShouldDetectStaticSingletonAssignmentInSourceNode()
    {
        var code = """
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;
                   using NPipeline.DataFlow;

                   public class BadService
                   {
                       public string Process(string input) => input.ToUpper();
                   }

                   public class BadSourceNode : SourceNode<int>
                   {
                       private static BadService _service; // Static field

                       public BadSourceNode()
                       {
                           _service = new BadService(); // Should trigger diagnostic - Static singleton assignment
                       }

                       public override IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
                       {
                           throw new NotImplementedException();
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == DependencyInjectionAnalyzer.DependencyInjectionAntiPatternId);
        Assert.True(hasDiagnostic, "Analyzer should detect static singleton assignment in SourceNode");
    }

    [Fact]
    public void ShouldDetectServiceLocatorPatternInSinkNode()
    {
        var code = """
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;
                   using NPipeline.DataFlow;
                   using System;

                   public class BadService
                   {
                       public string Process(string input) => input.ToUpper();
                   }

                   public class BadSinkNode : SinkNode<string>
                   {
                       private readonly IServiceProvider _serviceProvider;

                       public BadSinkNode(IServiceProvider serviceProvider)
                       {
                           _serviceProvider = serviceProvider;
                       }

                       public override Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           var badService = _serviceProvider.GetService(typeof(BadService)) as BadService; // Should trigger diagnostic
                           return Task.CompletedTask;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == DependencyInjectionAnalyzer.DependencyInjectionAntiPatternId);
        Assert.True(hasDiagnostic, "Analyzer should detect service locator pattern in SinkNode");
    }

    [Fact]
    public void ShouldNotTriggerForDtoInstantiation()
    {
        var code = """
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public record TestDto(string Value = "test");

                   public class GoodTransformNode : TransformNode<string, string>
                   {
                       public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           var dto = new TestDto(); // Should NOT trigger diagnostic - DTO instantiation is OK
                           return Task.FromResult(dto.Value);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == DependencyInjectionAnalyzer.DependencyInjectionAntiPatternId);
        Assert.False(hasDiagnostic, "Analyzer should not trigger for DTO instantiation");
    }

    [Fact]
    public void ShouldNotTriggerForConstructorInjection()
    {
        var code = """
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;
                   using NPipeline.DataFlow;

                   public class BadService
                   {
                       public string Process(string input) => input.ToUpper();
                   }

                   public class GoodSinkNode : SinkNode<string>
                   {
                       private readonly BadService _service;

                       public GoodSinkNode(BadService service) // Constructor injection - Should NOT trigger diagnostic
                       {
                           _service = service;
                       }

                       public override Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.CompletedTask;
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == DependencyInjectionAnalyzer.DependencyInjectionAntiPatternId);
        Assert.False(hasDiagnostic, "Analyzer should not trigger for constructor injection");
    }

    [Fact]
    public void ShouldNotTriggerForNonNodeClasses()
    {
        var code = """
                   public class BadService
                   {
                       public string Process(string input) => input.ToUpper();
                   }

                   public class NotANode
                   {
                       private readonly BadService _badService = new BadService(); // Should NOT trigger diagnostic - not a node

                       public string Process(string input)
                       {
                           return _badService.Process(input);
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == DependencyInjectionAnalyzer.DependencyInjectionAntiPatternId);
        Assert.False(hasDiagnostic, "Analyzer should not trigger for non-node classes");
    }

    [Fact]
    public void ShouldDetectMultipleAntiPatternsInSameClass()
    {
        var code = """
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;
                   using NPipeline.DataFlow;
                   using System;

                   public class BadService
                   {
                       public string Process(string input) => input.ToUpper();
                   }

                   public class VeryBadTransformNode : TransformNode<string, string>
                   {
                       private readonly BadService _badService = new BadService(); // Should trigger diagnostic - Direct instantiation
                       private static BadService _staticService; // Static field

                       public VeryBadTransformNode()
                       {
                           _staticService = new BadService(); // Should trigger diagnostic - Static singleton assignment
                       }

                       public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           var serviceProvider = context.Items["ServiceProvider"] as IServiceProvider;
                           var service = serviceProvider.GetService(typeof(BadService)) as BadService; // Should trigger diagnostic - Service locator
                           return Task.FromResult(service.Process(item));
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var antiPatternDiagnostics = diagnostics.Where(d => d.Id == DependencyInjectionAnalyzer.DependencyInjectionAntiPatternId).ToList();
        Assert.True(antiPatternDiagnostics.Count >= 3, "Analyzer should detect multiple anti-patterns in the same class");
    }

    [Fact]
    public void ShouldDetectInheritedNodeClasses()
    {
        var code = """
                   using NPipeline.Nodes;
                   using NPipeline.Pipeline;

                   public class BadService
                   {
                       public string Process(string input) => input.ToUpper();
                   }

                   public class CustomTransformNode : TransformNode<string, string>
                   {
                       private readonly BadService _badService = new BadService(); // Should trigger diagnostic

                       public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(_badService.Process(item));
                       }
                   }

                   public class InheritedTransformNode : CustomTransformNode
                   {
                       private readonly BadService _anotherBadService = new BadService(); // Should trigger diagnostic

                       public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
                       {
                           return Task.FromResult(_anotherBadService.Process(item));
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var antiPatternDiagnostics = diagnostics.Where(d => d.Id == DependencyInjectionAnalyzer.DependencyInjectionAntiPatternId).ToList();
        Assert.True(antiPatternDiagnostics.Count >= 2, "Analyzer should detect anti-patterns in inherited node classes");
    }

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        // Get the path to the NPipeline assembly
        var nPipelineAssemblyPath = typeof(TransformNode<,>).Assembly.Location;
        var nPipelineAnalyzersAssemblyPath = typeof(DependencyInjectionAnalyzer).Assembly.Location;

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Task).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(CancellationToken).Assembly.Location),
            MetadataReference.CreateFromFile(nPipelineAssemblyPath),
            MetadataReference.CreateFromFile(nPipelineAnalyzersAssemblyPath),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        var analyzer = new DependencyInjectionAnalyzer();
        var compilationWithAnalyzers = compilation.WithAnalyzers(ImmutableArray.Create<DiagnosticAnalyzer>(analyzer));
        var diagnostics = compilationWithAnalyzers.GetAnalyzerDiagnosticsAsync().Result;

        // Debug: Print all diagnostics
        Debug.WriteLine($"Total diagnostics found: {diagnostics.Length}");

        foreach (var diagnostic in diagnostics)
        {
            Debug.WriteLine($"Diagnostic: {diagnostic.Id} - {diagnostic.GetMessage()}");
        }

        return diagnostics;
    }
}
