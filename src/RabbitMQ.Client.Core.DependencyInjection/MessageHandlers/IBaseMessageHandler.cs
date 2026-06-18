namespace RabbitMQ.Client.Core.DependencyInjection.MessageHandlers
{
    /// <summary>
    /// A base interface that unites all message handlers.
    /// </summary>
    public interface IBaseMessageHandler
    {
        /// <summary>
        /// Consumer prefetch count (QoS) for this handler's dedicated channel.
        /// When set to null, the global PrefetchCount from RabbitMqServiceOptions is used.
        /// </summary>
        ushort? PrefetchCount { get; }
    }
}