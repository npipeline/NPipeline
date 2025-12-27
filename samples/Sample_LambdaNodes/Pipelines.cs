using NPipeline.Pipeline;

namespace Sample_LambdaNodes;

/// <summary>
///     Demonstrates the simplified lambda-based node syntax for creating simple pipelines.
/// </summary>
public class SimpleLambdaPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            () => Enumerable.Range(1, 10),
            "numberGenerator");

        var doubleNumbers = builder.AddTransform(
            (int x) => x * 2,
            "doubleNumbers");

        var addBase = builder.AddTransform(
            (int x) => x + 100,
            "addBase");

        var printSink = builder.AddSink(
            (int value) => Console.WriteLine($"Result: {value}"),
            "console");

        builder.Connect(source, doubleNumbers);
        builder.Connect(doubleNumbers, addBase);
        builder.Connect(addBase, printSink);
    }
}

/// <summary>
///     Demonstrates a hybrid approach using extracted testable functions.
/// </summary>
public class HybridApproachPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            () => new[] { 100m, 250m, 50m, 75m },
            "productPrices");

        var applyDiscount = builder.AddTransform<decimal, decimal>(
            CalculateDiscount,
            "applyDiscount");

        var formatPrice = builder.AddTransform<decimal, string>(
            FormatPrice,
            "formatPrice");

        var displaySink = builder.AddSink(
            (string price) => Console.WriteLine($"Discounted Price: {price}"),
            "display");

        builder.Connect(source, applyDiscount);
        builder.Connect(applyDiscount, formatPrice);
        builder.Connect<string>(formatPrice, displaySink);
    }

    public static decimal CalculateDiscount(decimal originalPrice)
    {
        return originalPrice * 0.9m;
    }

    public static string FormatPrice(decimal price)
    {
        return $"${price:F2}";
    }
}

/// <summary>
///     Demonstrates error handling with lambda nodes.
/// </summary>
public class ErrorHandlingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            () => new[] { "100", "200", "invalid", "300", "400" },
            "numberStrings");

        var parseNumbers = builder.AddTransform(
            (string s) =>
            {
                try
                {
                    return int.Parse(s);
                }
                catch (FormatException)
                {
                    Console.WriteLine($"⚠ Failed to parse: '{s}'");
                    return -1;
                }
            },
            "parseNumbers");

        var filterValid = builder.AddTransform(
            (int x) => x >= 0
                ? x
                : 0,
            "filterValid");

        var printSink = builder.AddSink(
            (int value) =>
            {
                if (value > 0)
                    Console.WriteLine($"✓ Parsed successfully: {value}");
            },
            "printResults");

        builder.Connect(source, parseNumbers);
        builder.Connect(parseNumbers, filterValid);
        builder.Connect(filterValid, printSink);
    }
}

/// <summary>
///     Demonstrates complex data transformations with objects.
/// </summary>
public class ComplexTransformationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            () => new[]
            {
                new Product(1, "Laptop", 1200m),
                new Product(2, "Mouse", 50m),
                new Product(3, "Keyboard", 100m),
                new Product(4, "Monitor", 350m),
            },
            "products");

        var createSale = builder.AddTransform(
            (Product p) =>
            {
                const decimal discountPercentage = 0.15m;
                var discountAmount = p.Price * discountPercentage;
                var discountedPrice = p.Price - discountAmount;

                return new ProductSale(
                    p.Id,
                    p.Name,
                    discountedPrice,
                    discountAmount);
            },
            "createSale");

        var displaySale = builder.AddSink(
            (ProductSale sale) =>
            {
                Console.WriteLine(
                    $"📦 {sale.ProductName}: ${sale.DiscountedPrice:F2} (Save ${sale.Savings:F2})");
            },
            "displaySale");

        builder.Connect(source, createSale);
        builder.Connect(createSale, displaySale);
    }

    public record Product(int Id, string Name, decimal Price);

    public record ProductSale(int ProductId, string ProductName, decimal DiscountedPrice, decimal Savings);
}
