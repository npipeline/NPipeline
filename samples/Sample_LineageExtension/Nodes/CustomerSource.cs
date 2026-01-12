using System.Globalization;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_LineageExtension.Nodes
{
    /// <summary>
    ///     Source node that generates customer data for the lineage tracking pipeline.
    ///     This node creates realistic customer profiles with various loyalty tiers.
    /// </summary>
    public class CustomerSource(int customerCount = 10, int? seed = null) : SourceNode<CustomerData>
    {
        /// <summary>
        ///     Generates a collection of customer data with realistic profiles.
        /// </summary>
        /// <param name="context">The pipeline execution context.</param>
        /// <param name="cancellationToken">Cancellation token to stop processing.</param>
        /// <returns>A data pipe containing the generated customer data.</returns>
        public override IDataPipe<CustomerData> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            Console.WriteLine($"[CustomerSource] Generating {customerCount} customer profiles...");

            var random = seed.HasValue ? new Random(seed.Value) : new Random();
            var customers = new List<CustomerData>();
            var baseDate = DateTime.UtcNow.AddDays(-365);

            var firstNames = new[] { "John", "Jane", "Mike", "Sarah", "David", "Emily", "Robert", "Lisa", "James", "Mary" };
            var lastNames = new[] { "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez" };

            // Generate customers with realistic data
            for (var i = 1; i <= customerCount; i++)
            {
                var firstName = firstNames[random.Next(firstNames.Length)];
                var lastName = lastNames[random.Next(lastNames.Length)];
                var fullName = string.Format(CultureInfo.InvariantCulture, "{0} {1}", firstName, lastName);
                var email = string.Format(CultureInfo.InvariantCulture, "{0}.{1}{2}@example.com", firstName.ToLowerInvariant(), lastName.ToLowerInvariant(), i);
                var phone = string.Format(CultureInfo.InvariantCulture, "+1-{0}-{1}-{2}", random.Next(100, 999), random.Next(100, 999), random.Next(1000, 9999));

                // Distribute loyalty tiers realistically
                var tierValue = random.Next(0, 100);
                var loyaltyTier = tierValue switch
                {
                    < 40 => LoyaltyTier.Bronze,
                    < 70 => LoyaltyTier.Silver,
                    < 90 => LoyaltyTier.Gold,
                    _ => LoyaltyTier.Platinum
                };

                // Lifetime value based on tier
                var lifetimeValue = loyaltyTier switch
                {
                    LoyaltyTier.Bronze => random.NextDecimal(100, 1000),
                    LoyaltyTier.Silver => random.NextDecimal(1000, 5000),
                    LoyaltyTier.Gold => random.NextDecimal(5000, 15000),
                    LoyaltyTier.Platinum => random.NextDecimal(15000, 50000),
                    _ => random.NextDecimal(100, 1000)
                };

                // Order count based on tier and lifetime value
                var orderCount = (int)(lifetimeValue / random.NextDecimal(50, 200));
                orderCount = Math.Max(1, orderCount);

                var registrationDate = baseDate.AddDays(random.Next(0, 300));

                var customer = new CustomerData(
                    customerId: i,
                    fullName: fullName,
                    email: email,
                    phone: phone,
                    loyaltyTier: loyaltyTier,
                    lifetimeValue: Math.Round(lifetimeValue, 2),
                    orderCount: orderCount,
                    registrationDate: registrationDate);

                customers.Add(customer);
            }

            Console.WriteLine($"[CustomerSource] Generated {customers.Count} customer profiles");
            return new InMemoryDataPipe<CustomerData>(customers, "CustomerSource");
        }
    }
}