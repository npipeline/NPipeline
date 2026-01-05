namespace Sample_SelfJoinNode;

/// <summary>
///     Represents sales data for a product in a specific year.
///     This is the base type used for self-join operations.
/// </summary>
/// <param name="ProductId">Unique identifier for the product.</param>
/// <param name="ProductName">Name of the product.</param>
/// <param name="Year">The sales year.</param>
/// <param name="Revenue">Total revenue for the product in that year.</param>
/// <param name="UnitsSold">Total units sold for the product in that year.</param>
/// <param name="Category">Product category.</param>
public sealed record SalesData(
    int ProductId,
    string ProductName,
    int Year,
    decimal Revenue,
    int UnitsSold,
    string Category
)
{
    /// <summary>
    ///     Gets the average price per unit.
    /// </summary>
    public decimal AveragePrice => UnitsSold > 0
        ? Revenue / UnitsSold
        : 0m;
}

/// <summary>
///     Represents the result of joining sales data from two different years.
///     This is the output type of the self-join operation.
/// </summary>
/// <param name="CurrentYearSales">Sales data for the current year.</param>
/// <param name="PreviousYearSales">Sales data for the previous year (may be null for LeftOuter join).</param>
public sealed record YearOverYearComparison(
    SalesData CurrentYearSales,
    SalesData? PreviousYearSales
)
{
    /// <summary>
    ///     Gets the product ID.
    /// </summary>
    public int ProductId => CurrentYearSales.ProductId;

    /// <summary>
    ///     Gets the product name.
    /// </summary>
    public string ProductName => CurrentYearSales.ProductName;

    /// <summary>
    ///     Gets the current year.
    /// </summary>
    public int CurrentYear => CurrentYearSales.Year;

    /// <summary>
    ///     Gets the previous year (if available).
    /// </summary>
    public int? PreviousYear => PreviousYearSales?.Year;

    /// <summary>
    ///     Gets the revenue growth percentage.
    ///     Returns null if there's no previous year data.
    /// </summary>
    public decimal? RevenueGrowthPercent
    {
        get
        {
            if (PreviousYearSales is null || PreviousYearSales.Revenue == 0)
                return null;

            return (CurrentYearSales.Revenue - PreviousYearSales.Revenue) / PreviousYearSales.Revenue * 100m;
        }
    }

    /// <summary>
    ///     Gets the units sold growth percentage.
    ///     Returns null if there's no previous year data.
    /// </summary>
    public decimal? UnitsGrowthPercent
    {
        get
        {
            if (PreviousYearSales is null || PreviousYearSales.UnitsSold == 0)
                return null;

            return (CurrentYearSales.UnitsSold - PreviousYearSales.UnitsSold) / (decimal)PreviousYearSales.UnitsSold * 100m;
        }
    }

    /// <summary>
    ///     Gets the revenue difference between years.
    /// </summary>
    public decimal RevenueDifference => PreviousYearSales is null
        ? CurrentYearSales.Revenue
        : CurrentYearSales.Revenue - PreviousYearSales.Revenue;

    /// <summary>
    ///     Gets the units sold difference between years.
    /// </summary>
    public int UnitsDifference => PreviousYearSales is null
        ? CurrentYearSales.UnitsSold
        : CurrentYearSales.UnitsSold - PreviousYearSales.UnitsSold;

    /// <summary>
    ///     Gets a human-readable growth status.
    /// </summary>
    public string GrowthStatus => RevenueGrowthPercent switch
    {
        null => "New Product",
        > 20 => "Strong Growth",
        > 0 => "Moderate Growth",
        0 => "Stable",
        < -20 => "Significant Decline",
        _ => "Decline",
    };
}

/// <summary>
///     Represents an aggregated sales summary by category.
/// </summary>
/// <param name="Category">Product category.</param>
/// <param name="TotalProducts">Total number of products in the category.</param>
/// <param name="GrowingProducts">Number of products with positive revenue growth.</param>
/// <param name="DecliningProducts">Number of products with negative revenue growth.</param>
/// <param name="NewProducts">Number of new products (no previous year data).</param>
/// <param name="AverageRevenueGrowth">Average revenue growth percentage across products.</param>
public sealed record CategorySummary(
    string Category,
    int TotalProducts,
    int GrowingProducts,
    int DecliningProducts,
    int NewProducts,
    decimal AverageRevenueGrowth
)
{
    /// <summary>
    ///     Gets the percentage of products showing growth.
    /// </summary>
    public decimal GrowthPercentage => TotalProducts > 0
        ? (decimal)GrowingProducts / TotalProducts * 100m
        : 0m;
}
