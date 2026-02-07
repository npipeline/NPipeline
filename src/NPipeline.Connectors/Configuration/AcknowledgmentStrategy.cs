namespace NPipeline.Connectors.Configuration
{
    /// <summary>
    /// Defines the strategy for acknowledging messages after processing.
    /// </summary>
    public enum AcknowledgmentStrategy
    {
        /// <summary>
        /// Automatically acknowledge messages immediately after successful sink processing.
        /// This is the default and provides the best developer experience.
        /// </summary>
        AutoOnSinkSuccess,

        /// <summary>
        /// Manually acknowledge messages by calling AcknowledgeAsync() on the message.
        /// Provides maximum control but requires explicit acknowledgment in transforms.
        /// </summary>
        Manual,

        /// <summary>
        /// Automatically acknowledge messages after a configurable delay.
        /// Useful for scenarios where you want to allow time for downstream processing.
        /// </summary>
        Delayed,

        /// <summary>
        /// Never acknowledge messages automatically. Messages remain in the queue
        /// until their visibility timeout expires. Use with caution.
        /// </summary>
        None
    }
}
