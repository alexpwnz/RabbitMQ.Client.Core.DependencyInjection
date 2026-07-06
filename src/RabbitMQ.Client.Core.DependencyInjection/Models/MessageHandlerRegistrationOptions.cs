using System;

namespace RabbitMQ.Client.Core.DependencyInjection.Models
{
    public class MessageHandlerRegistrationOptions
    {
        public MessageHandlerRegistrationOptions(Type handlerType, ushort? prefetchCount)
        {
            HandlerType = handlerType;
            PrefetchCount = prefetchCount;
        }

        public Type HandlerType { get; }

        public ushort? PrefetchCount { get; }
    }
}
