using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Core.DependencyInjection.MessageHandlers;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;

namespace Examples.ManualAck.MessageHandlers
{
    public class CustomMessageHandler : IAsyncMessageHandler
    {
        private readonly IProducingService _producingService;

        public CustomMessageHandler(IProducingService producingService)
        {
            _producingService = producingService;
        }

        public async Task Handle(MessageHandlingContext context, string matchingRoute)
        {
            var properties = new BasicProperties();
            if (context.Message.BasicProperties.Headers is not null)
            {
                properties.Headers = new Dictionary<string, object?>();
                foreach (var header in context.Message.BasicProperties.Headers)
                {
                    properties.Headers[header.Key] = header.Value;
                }
            }
            await _producingService.SendAsync(context.Message.Body, properties, "exchange", "other.routing.key");
        }
    }
}
