using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    public class ErrorProcessingService : IErrorProcessingService
    {
        private readonly IProducingService _producingService;
        private readonly IEnumerable<RabbitMqExchange> _exchanges;
        private readonly ILoggingService _loggingService;

        public ErrorProcessingService(
            IProducingService producingService,
            IEnumerable<RabbitMqExchange> exchanges,
            ILoggingService loggingService)
        {
            _producingService = producingService;
            _exchanges = exchanges;
            _loggingService = loggingService;
        }

        public virtual async Task HandleMessageProcessingFailure(MessageHandlingContext context, Exception exception)
        {
            var eventArgs = context.Message;
            if (context.AutoAckEnabled)
            {
                await context.AcknowledgeMessage().ConfigureAwait(false);
            }

            _loggingService.LogError(exception, $"An error occurred while processing received message with the delivery tag {eventArgs.DeliveryTag}.");
            await HandleFailedMessageProcessing(eventArgs).ConfigureAwait(false);
        }

        protected async Task HandleFailedMessageProcessing(BasicDeliverEventArgs eventArgs)
        {
            var exchange = _exchanges.FirstOrDefault(x => x.Name == eventArgs.Exchange);
            if (exchange is null)
            {
                _loggingService.LogWarning($"Could not detect an exchange \"{eventArgs.Exchange}\" to determine the necessity of resending the failed message. The message won't be re-queued");
                return;
            }
            
            if (!exchange.Options.RequeueFailedMessages)
            {
                _loggingService.LogWarning($"RequeueFailedMessages option for an exchange \"{eventArgs.Exchange}\" is disabled. The message won't be re-queued");
                return;
            }

            if (string.IsNullOrEmpty(exchange.Options.DeadLetterExchange))
            {
                _loggingService.LogWarning($"DeadLetterExchange has not been configured for an exchange \"{eventArgs.Exchange}\". The message won't be re-queued");
                return;
            }

            if (exchange.Options.RequeueTimeoutMilliseconds < 1)
            {
                _loggingService.LogWarning($"The value RequeueTimeoutMilliseconds for an exchange \"{eventArgs.Exchange}\" less than 1 millisecond. Configuration is invalid. The message won't be re-queued");
                return;
            }
            
            if (exchange.Options.RequeueAttempts < 1)
            {
                _loggingService.LogWarning($"The value RequeueAttempts for an exchange \"{eventArgs.Exchange}\" less than 1. Configuration is invalid. The message won't be re-queued");
                return;
            }

            var properties = new BasicProperties();
            if (eventArgs.BasicProperties.Headers is not null)
            {
                foreach (var header in eventArgs.BasicProperties.Headers)
                {
                    properties.Headers ??= new Dictionary<string, object?>();
                    properties.Headers[header.Key] = header.Value;
                }
            }

            if (properties.Headers is null)
            {
                properties.Headers = new Dictionary<string, object?>();
            }

            if (!properties.Headers.ContainsKey("re-queue-attempts"))
            {
                properties.Headers.Add("re-queue-attempts", 1);
                await RequeueMessage(eventArgs, properties, exchange.Options.RequeueTimeoutMilliseconds);
                return;
            }
            
            var currentAttempt = (int)properties.Headers["re-queue-attempts"]!;
            if (currentAttempt < exchange.Options.RequeueAttempts)
            {
                properties.Headers["re-queue-attempts"] = currentAttempt + 1;
                await RequeueMessage(eventArgs, properties, exchange.Options.RequeueTimeoutMilliseconds);
            }
            else
            {
                _loggingService.LogInformation("The failed message would not be re-queued. Attempts limit exceeded");   
            }
        }
        
        protected async Task RequeueMessage(BasicDeliverEventArgs eventArgs, BasicProperties properties, int timeoutMilliseconds)
        {
            await _producingService.SendAsync(eventArgs.Body, properties, eventArgs.Exchange, eventArgs.RoutingKey, timeoutMilliseconds);
            _loggingService.LogInformation("The failed message has been re-queued");
        }
    }
}