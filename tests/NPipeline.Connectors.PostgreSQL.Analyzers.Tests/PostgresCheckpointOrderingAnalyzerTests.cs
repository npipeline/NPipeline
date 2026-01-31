using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Connectors.Configuration;
using NPipeline.Connectors.PostgreSQL.Nodes;
using NPipeline.Nodes;

namespace NPipeline.Connectors.PostgreSQL.Analyzers.Tests;

/// <summary>
///     Tests for PostgresCheckpointOrderingAnalyzer.
/// </summary>
public sealed class PostgresCheckpointOrderingAnalyzerTests
{
    [Fact]
    public void ShouldDetectMissingOrderByWithCheckpointingEnabled()
    {
        var code = """
                   using NPipeline.Connectors.PostgreSQL.Configuration;
                   using NPipeline.Connectors.PostgreSQL.Nodes;
                   using NPipeline.Connectors.Configuration;

                   public class TestPipeline
                   {
                       public void CreateSource()
                       {
                           var source = new PostgresSourceNode<MyRecord>(
                               "Host=localhost;Database=test",
                               "SELECT id, name FROM my_table",
                               configuration: new PostgresConfiguration
                               {
                                   CheckpointStrategy = CheckpointStrategy.Offset
                               }
                           );
                       }
                   }

                   public class MyRecord { public int Id { get; set; } public string Name { get; set; } }
                   """;

        var diagnostics = GetDiagnostics(code);

        // Debug: print all diagnostics
        Console.WriteLine($"Total diagnostics: {diagnostics.Count()}");

        foreach (var d in diagnostics)
        {
            Console.WriteLine($"Diagnostic: {d.Id} - {d.GetMessage()}");
        }

        var hasDiagnostic = diagnostics.Any(d => d.Id == PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId);
        Assert.True(hasDiagnostic, "Analyzer should detect missing ORDER BY clause with checkpointing enabled");
    }

    [Fact]
    public void ShouldNotTriggerWhenCheckpointingIsDisabled()
    {
        var code = """
                   using NPipeline.Connectors.PostgreSQL.Configuration;
                   using NPipeline.Connectors.PostgreSQL.Nodes;
                   using NPipeline.Connectors.Configuration;

                   public class TestPipeline
                   {
                       public void CreateSource()
                       {
                           var source = new PostgresSourceNode<MyRecord>(
                               "Host=localhost;Database=test",
                               "SELECT id, name FROM my_table",
                               configuration: new PostgresConfiguration
                               {
                                   CheckpointStrategy = CheckpointStrategy.None
                               }
                           );
                       }
                   }

                   public class MyRecord { public int Id { get; set; } public string Name { get; set; } }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId);
        Assert.False(hasDiagnostic, "Analyzer should not trigger when checkpointing is disabled");
    }

    [Fact]
    public void ShouldNotTriggerWhenOrderByIsPresent()
    {
        var code = """
                   using NPipeline.Connectors.PostgreSQL.Configuration;
                   using NPipeline.Connectors.PostgreSQL.Nodes;
                   using NPipeline.Connectors.Configuration;

                   public class TestPipeline
                   {
                       public void CreateSource()
                       {
                           var source = new PostgresSourceNode<MyRecord>(
                               "Host=localhost;Database=test",
                               "SELECT id, name FROM my_table ORDER BY id",
                               configuration: new PostgresConfiguration
                               {
                                   CheckpointStrategy = CheckpointStrategy.Offset
                               }
                           );
                       }
                   }

                   public class MyRecord { public int Id { get; set; } public string Name { get; set; } }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId);
        Assert.False(hasDiagnostic, "Analyzer should not trigger when ORDER BY clause is present");
    }

    [Fact]
    public void ShouldNotTriggerForInMemoryCheckpointing()
    {
        var code = """
                   using NPipeline.Connectors.PostgreSQL.Configuration;
                   using NPipeline.Connectors.PostgreSQL.Nodes;
                   using NPipeline.Connectors.Configuration;

                   public class TestPipeline
                   {
                       public void CreateSource()
                       {
                           var source = new PostgresSourceNode<MyRecord>(
                               "Host=localhost;Database=test",
                               "SELECT id, name FROM my_table",
                               configuration: new PostgresConfiguration
                               {
                                   CheckpointStrategy = CheckpointStrategy.InMemory
                               }
                           );
                       }
                   }

                   public class MyRecord { public int Id { get; set; } public string Name { get; set; } }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId);
        Assert.False(hasDiagnostic, "Analyzer should not trigger for InMemory checkpointing");
    }

    [Fact]
    public void ShouldDetectMissingOrderByWithKeyBasedCheckpointing()
    {
        var code = """
                   using NPipeline.Connectors.PostgreSQL.Configuration;
                   using NPipeline.Connectors.PostgreSQL.Nodes;
                   using NPipeline.Connectors.Configuration;

                   public class TestPipeline
                   {
                       public void CreateSource()
                       {
                           var source = new PostgresSourceNode<MyRecord>(
                               "Host=localhost;Database=test",
                               "SELECT id, name FROM my_table",
                               configuration: new PostgresConfiguration
                               {
                                   CheckpointStrategy = CheckpointStrategy.KeyBased
                               }
                           );
                       }
                   }

                   public class MyRecord { public int Id { get; set; } public string Name { get; set; } }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId);
        Assert.True(hasDiagnostic, "Analyzer should detect missing ORDER BY with KeyBased checkpointing");
    }

    [Fact]
    public void ShouldDetectMissingOrderByWithCursorCheckpointing()
    {
        var code = """
                   using NPipeline.Connectors.PostgreSQL.Configuration;
                   using NPipeline.Connectors.PostgreSQL.Nodes;
                   using NPipeline.Connectors.Configuration;

                   public class TestPipeline
                   {
                       public void CreateSource()
                       {
                           var source = new PostgresSourceNode<MyRecord>(
                               "Host=localhost;Database=test",
                               "SELECT id, name FROM my_table",
                               configuration: new PostgresConfiguration
                               {
                                   CheckpointStrategy = CheckpointStrategy.Cursor
                               }
                           );
                       }
                   }

                   public class MyRecord { public int Id { get; set; } public string Name { get; set; } }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId);
        Assert.True(hasDiagnostic, "Analyzer should detect missing ORDER BY with Cursor checkpointing");
    }

    [Fact]
    public void ShouldNotTriggerForInterpolatedStrings()
    {
        var code = """
                   using NPipeline.Connectors.PostgreSQL.Configuration;
                   using NPipeline.Connectors.PostgreSQL.Nodes;
                   using NPipeline.Connectors.Configuration;

                   public class TestPipeline
                   {
                       public void CreateSource(string tableName)
                       {
                           var source = new PostgresSourceNode<MyRecord>(
                               "Host=localhost;Database=test",
                               $"SELECT id, name FROM {tableName}",
                               configuration: new PostgresConfiguration
                               {
                                   CheckpointStrategy = CheckpointStrategy.Offset
                               }
                           );
                       }
                   }

                   public class MyRecord { public int Id { get; set; } public string Name { get; set; } }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId);
        Assert.False(hasDiagnostic, "Analyzer should not trigger for interpolated strings (cannot analyze)");
    }

    [Fact]
    public void ShouldNotTriggerWhenConfigurationIsNull()
    {
        var code = """
                   using NPipeline.Connectors.PostgreSQL.Nodes;

                   public class TestPipeline
                   {
                       public void CreateSource()
                       {
                           var source = new PostgresSourceNode<MyRecord>(
                               "Host=localhost;Database=test",
                               "SELECT id, name FROM my_table",
                               configuration: null
                           );
                       }
                   }

                   public class MyRecord { public int Id { get; set; } public string Name { get; set; } }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId);
        Assert.False(hasDiagnostic, "Analyzer should not trigger when configuration is null");
    }

    [Fact]
    public void ShouldDetectOrderByInCaseInsensitiveManner()
    {
        var code = """
                   using NPipeline.Connectors.PostgreSQL.Configuration;
                   using NPipeline.Connectors.PostgreSQL.Nodes;
                   using NPipeline.Connectors.Configuration;

                   public class TestPipeline
                   {
                       public void CreateSource()
                       {
                           var source = new PostgresSourceNode<MyRecord>(
                               "Host=localhost;Database=test",
                               "SELECT id, name FROM my_table order by id",
                               configuration: new PostgresConfiguration
                               {
                                   CheckpointStrategy = CheckpointStrategy.Offset
                               }
                           );
                       }
                   }

                   public class MyRecord { public int Id { get; set; } public string Name { get; set; } }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId);
        Assert.False(hasDiagnostic, "Analyzer should recognize ORDER BY in lowercase");
    }

    [Fact]
    public void ShouldNotAnalyzeNonPostgresSourceNode()
    {
        var code = """
                   public class TestSource
                   {
                       public void CreateSource()
                       {
                           var source = new object();
                       }
                   }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId);
        Assert.False(hasDiagnostic, "Analyzer should not analyze non-PostgresSourceNode types");
    }

    [Fact]
    public void AnalyzerShouldHaveCorrectDiagnosticId()
    {
        var analyzer = new PostgresCheckpointOrderingAnalyzer();
        var supportedDiagnostics = analyzer.SupportedDiagnostics;

        Assert.Single(supportedDiagnostics);
        var diagnostic = Assert.Single(supportedDiagnostics);
        Assert.Equal(PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId, diagnostic.Id);
    }

    [Fact]
    public void ShouldDetectMissingOrderByWithDirectCheckpointStrategy()
    {
        var code = """
                   using NPipeline.Connectors.PostgreSQL.Configuration;
                   using NPipeline.Connectors.PostgreSQL.Nodes;
                   using NPipeline.Connectors.Configuration;

                   public class TestPipeline
                   {
                       public void CreateSource()
                       {
                           var source = new PostgresSourceNode<MyRecord>(
                               "Host=localhost;Database=test",
                               "SELECT id, name FROM my_table",
                               configuration: new PostgresConfiguration
                               {
                                   CheckpointStrategy = CheckpointStrategy.Offset
                               }
                           );
                       }
                   }

                   public class MyRecord { public int Id { get; set; } public string Name { get; set; } }
                   """;

        var diagnostics = GetDiagnostics(code);

        // Debug: print all diagnostics
        Console.WriteLine($"Total diagnostics: {diagnostics.Count()}");

        foreach (var d in diagnostics)
        {
            Console.WriteLine($"Diagnostic: {d.Id} - {d.GetMessage()}");
        }

        var hasDiagnostic = diagnostics.Any(d => d.Id == PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId);
        Assert.True(hasDiagnostic, "Analyzer should detect missing ORDER BY with Offset checkpointing");
    }

    [Fact]
    public void ShouldDetectMissingOrderByWithOrderByInSubquery()
    {
        var code = """
                   using NPipeline.Connectors.PostgreSQL.Configuration;
                   using NPipeline.Connectors.PostgreSQL.Nodes;
                   using NPipeline.Connectors.Configuration;

                   public class TestPipeline
                   {
                       public void CreateSource()
                       {
                           var source = new PostgresSourceNode<MyRecord>(
                               "Host=localhost;Database=test",
                               "SELECT * FROM (SELECT id, name FROM my_table ORDER BY id) AS subq",
                               configuration: new PostgresConfiguration
                               {
                                   CheckpointStrategy = CheckpointStrategy.Offset
                               }
                           );
                       }
                   }

                   public class MyRecord { public int Id { get; set; } public string Name { get; set; } }
                   """;

        var diagnostics = GetDiagnostics(code);

        var hasDiagnostic = diagnostics.Any(d => d.Id == PostgresCheckpointOrderingAnalyzer.PostgresCheckpointOrderingId);

        // The analyzer uses a simple regex that will find ORDER BY anywhere in the query
        Assert.False(hasDiagnostic, "Analyzer should recognize ORDER BY in subquery");
    }

    private static IEnumerable<Diagnostic> GetDiagnostics(string code)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(code);

        // Find System.Runtime assembly location
        var runtimeAssemblyPath = typeof(object).Assembly.Location;
        var runtimeDir = Path.GetDirectoryName(runtimeAssemblyPath);
        var systemRuntimePath = Path.Combine(runtimeDir ?? "", "System.Runtime.dll");

        var references = new[]
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(DiagnosticAnalyzer).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(PostgresCheckpointOrderingAnalyzer).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(PostgresSourceNode<>).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(SourceNode<>).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(CheckpointStrategy).Assembly.Location),
            MetadataReference.CreateFromFile(systemRuntimePath),
        };

        var compilation = CSharpCompilation.Create("TestAssembly")
            .AddReferences(references)
            .AddSyntaxTrees(syntaxTree);

        // Check for compilation errors
        var compilationDiagnostics = compilation.GetDiagnostics();

        foreach (var d in compilationDiagnostics)
        {
            Console.WriteLine($"Compilation: {d.Severity} {d.Id} - {d.GetMessage()} at {d.Location}");
        }

        // Debug: Check what types the semantic model can find
        var semanticModel = compilation.GetSemanticModel(syntaxTree);
        var objectCreations = syntaxTree.GetRoot().DescendantNodes().OfType<ObjectCreationExpressionSyntax>();

        foreach (var oc in objectCreations)
        {
            var typeInfo = semanticModel.GetTypeInfo(oc);

            Console.WriteLine(
                $"Object creation: {oc.Type} - Type: {typeInfo.Type} - Type name: {typeInfo.Type?.Name} - Namespace: {typeInfo.Type?.ContainingNamespace}");
        }

        var analyzer = new PostgresCheckpointOrderingAnalyzer();
        var compilation2 = compilation.WithAnalyzers([analyzer]);
        var diagnostics = compilation2.GetAnalyzerDiagnosticsAsync().Result;

        return diagnostics;
    }
}
