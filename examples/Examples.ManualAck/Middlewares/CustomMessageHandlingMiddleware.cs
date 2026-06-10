using System;
using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.Middlewares;
using RabbitMQ.Client.Core.DependencyInjection.Models;

namespace Examples.ManualAck.Middlewares
{
    public class CustomMessageHandlingMiddleware : IMessageHandlingMiddleware
    {
        public async Task Handle(MessageHandlingContext context, Func<Task> next)
        {
            // Execute the next action in the middleware pipeline.
            // Message handlers will be executed.
            await next();
            
            await context.AcknowledgeMessage().ConfigureAwait(false);
        }

        public async Task HandleError(MessageHandlingContext context, Exception exception, Func<Task> next)
        {
            await next().ConfigureAwait(false);
            
            await context.AcknowledgeMessage().ConfigureAwait(false);
            throw exception;
        }
    }
}