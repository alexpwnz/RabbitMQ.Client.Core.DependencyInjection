using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.MessageHandlers;
using RabbitMQ.Client.Core.DependencyInjection.Middlewares;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Core.DependencyInjection.Tracing;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    public class MessageHandlingPipelineExecutingService : IMessageHandlingPipelineExecutingService
    {
        private readonly IMessageHandlingService _messageHandlingService;
        private readonly IErrorProcessingService _errorProcessingService;
        private readonly IEnumerable<IMessageHandlingMiddleware> _messageHandlingMiddlewares;
        private readonly ITracingService _tracingService;

        public MessageHandlingPipelineExecutingService(
            IMessageHandlingService messageHandlingService,
            IErrorProcessingService errorProcessingService,
            IEnumerable<IMessageHandlingMiddleware> messageHandlingMiddlewares)
            : this(messageHandlingService, errorProcessingService, messageHandlingMiddlewares, new NullTracingService())
        {
        }

        public MessageHandlingPipelineExecutingService(
            IMessageHandlingService messageHandlingService,
            IErrorProcessingService errorProcessingService,
            IEnumerable<IMessageHandlingMiddleware> messageHandlingMiddlewares,
            ITracingService tracingService)
        {
            _messageHandlingService = messageHandlingService;
            _errorProcessingService = errorProcessingService;
            _messageHandlingMiddlewares = messageHandlingMiddlewares;
            _tracingService = tracingService;
        }

        public async Task Execute(MessageHandlingContext context)
        {
            var activity = _tracingService.StartConsumeActivity(context.Message);
            try
            {
                try
                {
                    await ExecutePipeline(context).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    await ExecuteFailurePipeline(context, exception).ConfigureAwait(false);
                }
            }
            finally
            {
                _tracingService.StopActivity(activity);
            }
        }

        public async Task ExecuteForHandler(MessageHandlingContext context, IBaseMessageHandler handler, string matchingRoute)
        {
            var activity = _tracingService.StartConsumeActivity(context.Message);
            try
            {
                try
                {
                    await ExecutePipelineForHandler(context, handler, matchingRoute).ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    await ExecuteFailurePipeline(context, exception).ConfigureAwait(false);
                }
            }
            finally
            {
                _tracingService.StopActivity(activity);
            }
        }

        private async Task ExecutePipeline(MessageHandlingContext context)
        {
            if (!_messageHandlingMiddlewares.Any())
            {
                await _messageHandlingService.HandleMessageReceivingEvent(context);
                return;
            }

            Func<Task> handleFunction = async () => await _messageHandlingService.HandleMessageReceivingEvent(context);
            foreach (var middleware in _messageHandlingMiddlewares)
            {
                var previousHandleFunction = handleFunction;
                handleFunction = async () => await middleware.Handle(context, previousHandleFunction);
            }

            await handleFunction().ConfigureAwait(false);
        }

        private async Task ExecutePipelineForHandler(MessageHandlingContext context, IBaseMessageHandler handler, string matchingRoute)
        {
            if (!_messageHandlingMiddlewares.Any())
            {
                await ExecuteHandlerDirectly(handler, context, matchingRoute);
                return;
            }

            Func<Task> handleFunction = async () => await ExecuteHandlerDirectly(handler, context, matchingRoute);
            foreach (var middleware in _messageHandlingMiddlewares)
            {
                var previousHandleFunction = handleFunction;
                handleFunction = async () => await middleware.Handle(context, previousHandleFunction);
            }

            await handleFunction().ConfigureAwait(false);
        }

        private static async Task ExecuteHandlerDirectly(IBaseMessageHandler handler, MessageHandlingContext context, string matchingRoute)
        {
            switch (handler)
            {
                case IMessageHandler messageHandler:
                    messageHandler.Handle(context, matchingRoute);
                    break;
                case IAsyncMessageHandler asyncMessageHandler:
                    await asyncMessageHandler.Handle(context, matchingRoute);
                    break;
                default:
                    throw new NotSupportedException($"The type {handler.GetType()} of message handler is not supported.");
            }
        }

        private async Task ExecuteFailurePipeline(MessageHandlingContext context, Exception exception)
        {
            if (!_messageHandlingMiddlewares.Any())
            {
                await _errorProcessingService.HandleMessageProcessingFailure(context, exception);
                return;
            }

            Func<Task> handleFunction = async () => await _errorProcessingService.HandleMessageProcessingFailure(context, exception);
            foreach (var middleware in _messageHandlingMiddlewares)
            {
                var previousHandleFunction = handleFunction;
                handleFunction = async () => await middleware.HandleError(context, exception, previousHandleFunction);
            }

            await handleFunction().ConfigureAwait(false);
        }
    }
}
