using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Nodes;
using NPipeline.DataFlow.DataPipes;

namespace Sample_RedshiftConnector;

public sealed class RedshiftConnectorPipeline
{
    private readonly string _awsRegion;
    private readonly string _connectionString;
    private readonly string _iamRoleArn;
    private readonly string _s3BucketName;

    public RedshiftConnectorPipeline(string connectionString, string s3BucketName, string iamRoleArn, string awsRegion)
    {
        _connectionString = connectionString;
        _s3BucketName = s3BucketName;
        _iamRoleArn = iamRoleArn;
        _awsRegion = awsRegion;
    }

    public static string GetDescription()
    {
        return "Reads order_events from Redshift → transforms → writes order_summaries via COPY FROM S3.";
    }

    public async Task ExecuteAsync(IServiceProvider _, CancellationToken cancellationToken)
    {
        // ── Step 1: Read from Redshift with streaming ───────────────────────────────
        Console.WriteLine("Step 1: Reading from order_events table...");
        Console.WriteLine("-------------------------------------------");

        var sourceConfig = new RedshiftConfiguration
        {
            StreamResults = true,
            FetchSize = 10_000,
        };

        var source = new RedshiftSourceNode<OrderEvent>(
            _connectionString,
            """
            SELECT order_id, customer_id, product_sku, quantity, unit_price,
                   ordered_at, status
            FROM   public.order_events
            WHERE  status IN ('completed', 'shipped')
            ORDER  BY ordered_at
            """,
            null,
            sourceConfig);

        // Read and transform the data
        var orderSummaries = new List<OrderSummary>();
        var pipe = source.Initialize(null!, cancellationToken);

        await foreach (var orderEvent in pipe.WithCancellation(cancellationToken))
        {
            var summary = new OrderSummary
            {
                OrderId = orderEvent.OrderId,
                CustomerId = orderEvent.CustomerId,
                Revenue = orderEvent.Quantity * orderEvent.UnitPrice,
                ItemCount = orderEvent.Quantity,
                OrderedAt = orderEvent.OrderedAt,
                Status = orderEvent.Status,
                ProcessedAt = DateTime.UtcNow,
            };

            orderSummaries.Add(summary);
            Console.WriteLine($"  Order {summary.OrderId}: Customer={summary.CustomerId}, Revenue=${summary.Revenue:F2}");
        }

        Console.WriteLine($"  ✓ Read {orderSummaries.Count} order(s)");
        Console.WriteLine();

        // ── Step 2: Write to Redshift with COPY FROM S3 + upsert ───────────────────
        Console.WriteLine("Step 2: Writing to order_summaries table...");
        Console.WriteLine("--------------------------------------------");

        var sinkConfig = new RedshiftConfiguration
        {
            WriteStrategy = RedshiftWriteStrategy.CopyFromS3,
            BatchSize = 5_000,
            UseTransaction = false,
            UseUpsert = true,
            UpsertKeyColumns = ["order_id"],
            S3BucketName = _s3BucketName,
            IamRoleArn = _iamRoleArn,
            AwsRegion = _awsRegion,
            PurgeS3FilesAfterCopy = true,
        };

        var sink = new RedshiftSinkNode<OrderSummary>(
            _connectionString,
            "order_summaries",
            RedshiftWriteStrategy.CopyFromS3,
            sinkConfig);

        var dataPipe = new InMemoryDataPipe<OrderSummary>(orderSummaries);
        await sink.ExecuteAsync(dataPipe, null!, cancellationToken);

        Console.WriteLine($"  ✓ Wrote {orderSummaries.Count} order summar(y/ies) via COPY FROM S3 upsert");
        Console.WriteLine();
    }
}
