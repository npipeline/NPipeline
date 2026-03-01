#pragma warning disable CS0618 // Type or member is obsolete
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;

namespace NPipeline.Connectors.Aws.Redshift.Analyzers.Tests;

public class RedshiftConfigurationAnalyzerTests
{
    private const string RedshiftConfigurationStubs = @"
namespace NPipeline.Connectors.Aws.Redshift.Configuration
{
    public enum RedshiftWriteStrategy
    {
        PerRow,
        Batch,
        CopyFromS3
    }

    public class RedshiftConfiguration
    {
        public RedshiftWriteStrategy WriteStrategy { get; set; }
        public string? IamRoleArn { get; set; }
        public string? S3BucketName { get; set; }
        public bool UseUpsert { get; set; }
        public string[]? UpsertKeyColumns { get; set; }
        public int BatchSize { get; set; }
    }
}";

    [Fact]
    public async Task CopyFromS3_WithoutIamRoleArn_ReportsREDSHIFT001()
    {
        var test = @"
using NPipeline.Connectors.Aws.Redshift.Configuration;

class Test
{
    void Method()
    {
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            S3BucketName = ""my-bucket""
        };
    }
}";

        var expected = new DiagnosticResult(DiagnosticIds.MissingIamRoleArn, DiagnosticSeverity.Error)
            .WithLocation(8, 22);

        await TestAsync(test, expected);
    }

    [Fact]
    public async Task CopyFromS3_WithoutS3BucketName_ReportsREDSHIFT002()
    {
        var test = @"
using NPipeline.Connectors.Aws.Redshift.Configuration;

class Test
{
    void Method()
    {
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            IamRoleArn = ""arn:aws:iam::123:role/MyRole""
        };
    }
}";

        var expected = new DiagnosticResult(DiagnosticIds.MissingS3BucketName, DiagnosticSeverity.Error)
            .WithLocation(8, 22);

        await TestAsync(test, expected);
    }

    [Fact]
    public async Task UseUpsert_WithoutKeyColumns_ReportsREDSHIFT003()
    {
        var test = @"
using NPipeline.Connectors.Aws.Redshift.Configuration;

class Test
{
    void Method()
    {
        var config = new RedshiftConfiguration
        {
            UseUpsert = true
        };
    }
}";

        var expected = new DiagnosticResult(DiagnosticIds.MissingUpsertKeyColumns, DiagnosticSeverity.Error)
            .WithLocation(8, 22);

        await TestAsync(test, expected);
    }

    [Fact]
    public async Task BatchStrategy_WithLargeBatchSize_ReportsREDSHIFT004()
    {
        var test = @"
using NPipeline.Connectors.Aws.Redshift.Configuration;

class Test
{
    void Method()
    {
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.Batch,
            BatchSize = 50000
        };
    }
}";

        var expected = new DiagnosticResult(DiagnosticIds.ConsiderCopyFromS3ForLargeBatches, DiagnosticSeverity.Warning)
            .WithLocation(8, 22)
            .WithArguments(50000);

        await TestAsync(test, expected);
    }

    [Fact]
    public async Task CopyFromS3_WithAllRequiredProperties_NoDiagnostics()
    {
        var test = @"
using NPipeline.Connectors.Aws.Redshift.Configuration;

class Test
{
    void Method()
    {
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            S3BucketName = ""my-bucket"",
            IamRoleArn = ""arn:aws:iam::123:role/MyRole""
        };
    }
}";

        await TestAsync(test);
    }

    [Fact]
    public async Task UseUpsert_WithKeyColumns_NoDiagnostics()
    {
        var test = @"
using NPipeline.Connectors.Aws.Redshift.Configuration;

class Test
{
    void Method()
    {
        var config = new RedshiftConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new[] { ""id"" }
        };
    }
}";

        await TestAsync(test);
    }

    [Fact]
    public async Task CopyFromS3_WithEmptyIamRoleArn_ReportsREDSHIFT001()
    {
        var test = @"
using NPipeline.Connectors.Aws.Redshift.Configuration;

class Test
{
    void Method()
    {
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            S3BucketName = ""my-bucket"",
            IamRoleArn = """"
        };
    }
}";

        var expected = new DiagnosticResult(DiagnosticIds.MissingIamRoleArn, DiagnosticSeverity.Error)
            .WithLocation(8, 22);

        await TestAsync(test, expected);
    }

    [Fact]
    public async Task CopyFromS3_WithNullIamRoleArn_ReportsREDSHIFT001()
    {
        var test = @"
using NPipeline.Connectors.Aws.Redshift.Configuration;

class Test
{
    void Method()
    {
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            S3BucketName = ""my-bucket"",
            IamRoleArn = null
        };
    }
}";

        var expected = new DiagnosticResult(DiagnosticIds.MissingIamRoleArn, DiagnosticSeverity.Error)
            .WithLocation(8, 22);

        await TestAsync(test, expected);
    }

    [Fact]
    public async Task UseUpsert_WithEmptyKeyColumns_ReportsREDSHIFT003()
    {
        var test = @"
using NPipeline.Connectors.Aws.Redshift.Configuration;

class Test
{
    void Method()
    {
        var config = new RedshiftConfiguration
        {
            UseUpsert = true,
            UpsertKeyColumns = new string[] { }
        };
    }
}";

        var expected = new DiagnosticResult(DiagnosticIds.MissingUpsertKeyColumns, DiagnosticSeverity.Error)
            .WithLocation(8, 22);

        await TestAsync(test, expected);
    }

    [Fact]
    public async Task BatchStrategy_WithSmallBatchSize_NoDiagnostics()
    {
        var test = @"
using NPipeline.Connectors.Aws.Redshift.Configuration;

class Test
{
    void Method()
    {
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.Batch,
            BatchSize = 5000
        };
    }
}";

        await TestAsync(test);
    }

    [Fact]
    public async Task CopyFromS3_WithLargeBatchSize_NoREDSHIFT004()
    {
        var test = @"
using NPipeline.Connectors.Aws.Redshift.Configuration;

class Test
{
    void Method()
    {
        var config = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            S3BucketName = ""my-bucket"",
            IamRoleArn = ""arn:aws:iam::123:role/MyRole"",
            BatchSize = 50000
        };
    }
}";

        // Should not report REDSHIFT004 because CopyFromS3 is already being used
        await TestAsync(test);
    }

    private static async Task TestAsync(string source, params DiagnosticResult[] expected)
    {
        var test = new CSharpAnalyzerTest<RedshiftConfigurationAnalyzer, DefaultVerifier>
        {
            TestCode = source,
        };

        test.TestState.Sources.Add(RedshiftConfigurationStubs);

        test.ExpectedDiagnostics.AddRange(expected);
        await test.RunAsync();
    }
}
#pragma warning restore CS0618 // Type or member is obsolete
