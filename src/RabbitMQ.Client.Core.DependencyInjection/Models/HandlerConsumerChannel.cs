using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.MessageHandlers;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Models
{
    internal class HandlerConsumerChannel : IDisposable
    {
        public IConnection Connection { get; }

        public IChannel Channel { get; }

        public AsyncEventingBasicConsumer Consumer { get; }

        public IBaseMessageHandler Handler { get; }

        public string QueueName { get; }

        public string Exchange { get; }

        public IList<string> RoutingKeys { get; }

        internal AsyncEventHandler<BasicDeliverEventArgs>? ReceivedHandler { get; set; }

        public string? ConsumerTag { get; set; }

        public HandlerConsumerChannel(
            IConnection connection,
            IChannel channel,
            AsyncEventingBasicConsumer consumer,
            IBaseMessageHandler handler,
            string queueName,
            string exchange,
            IList<string> routingKeys)
        {
            Connection = connection;
            Channel = channel;
            Consumer = consumer;
            Handler = handler;
            QueueName = queueName;
            Exchange = exchange;
            RoutingKeys = routingKeys;
        }

        public void Dispose()
        {
            Channel?.Dispose();
        }
    }
}
