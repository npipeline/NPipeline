; Shipped analyzer release file
; <https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md>

## Release 1.0

### New Rules

 Rule ID     | Category      | Severity | Notes
-------------|---------------|----------|-------------------------------------------------------
 REDSHIFT001 | Configuration | Error    | Missing IamRoleArn when WriteStrategy is CopyFromS3
 REDSHIFT002 | Configuration | Error    | Missing S3BucketName when WriteStrategy is CopyFromS3
 REDSHIFT003 | Configuration | Error    | Missing UpsertKeyColumns when UseUpsert is true
 REDSHIFT004 | Performance   | Warning  | Consider CopyFromS3 for large batch sizes
