using RabbitMQ.Client.Core.DependencyInjection.Models;

namespace RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces
{
    internal interface IConsumingServiceDeclaration
    {
        void AddHandlerConsumer(HandlerConsumerChannel handlerConsumer);
    }
}
