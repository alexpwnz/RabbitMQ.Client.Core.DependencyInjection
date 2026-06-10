using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client.Core.DependencyInjection.Configuration;
using RabbitMQ.Client.Core.DependencyInjection.Exceptions;
using RabbitMQ.Client.Core.DependencyInjection.InternalExtensions.Validation;
using RabbitMQ.Client.Core.DependencyInjection.Middlewares;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    public abstract class BaseBatchMessageHandler : IHostedService, IDisposable
    {
        public IConnection? Connection { get; private set; }

        public IChannel? Channel { get;  private set; }

        public virtual uint PrefetchSize { get; set; } = 0;

        public abstract string QueueName { get; set; }

        public abstract ushort PrefetchCount { get; set; }

        public virtual TimeSpan? MessageHandlingPeriod { get; set; }

        private readonly IRabbitMqConnectionFactory _rabbitMqConnectionFactory;
        private readonly RabbitMqServiceOptions _serviceOptions;
        private readonly IEnumerable<IBatchMessageHandlingMiddleware> _batchMessageHandlingMiddlewares;
        private readonly ILoggingService _loggingService;

        private readonly ConcurrentBag<BasicDeliverEventArgs> _messages = new ConcurrentBag<BasicDeliverEventArgs>();
        private Timer? _timer;
        private readonly object _lock = new object();
        private bool _disposed = false;

        protected BaseBatchMessageHandler(
            IRabbitMqConnectionFactory rabbitMqConnectionFactory,
            IEnumerable<BatchConsumerConnectionOptions> batchConsumerConnectionOptions,
            IEnumerable<IBatchMessageHandlingMiddleware> batchMessageHandlingMiddlewares,
            ILoggingService loggingService)
        {
            var optionsContainer = batchConsumerConnectionOptions.FirstOrDefault(x => x.Type == GetType());
            if (optionsContainer is null)
            {
                throw new ArgumentNullException($"Client connection options for {nameof(BaseBatchMessageHandler)} has not been found.", nameof(batchConsumerConnectionOptions));
            }

            _serviceOptions = optionsContainer.ServiceOptions;
            _rabbitMqConnectionFactory = rabbitMqConnectionFactory;
            _batchMessageHandlingMiddlewares = batchMessageHandlingMiddlewares;
            _loggingService = loggingService;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            ValidateProperties();
            _loggingService.LogInformation($"Batch message handler {GetType()} has been started.");
            Connection = (await _rabbitMqConnectionFactory.CreateRabbitMqConnectionAsync(_serviceOptions).ConfigureAwait(false)).EnsureIsNotNull();
            Channel = (await Connection.CreateChannelAsync().ConfigureAwait(false)).EnsureIsNotNull();
            await Channel.BasicQosAsync(PrefetchSize, PrefetchCount, false, default).ConfigureAwait(false);

            if (MessageHandlingPeriod != null)
            {
                _timer = new Timer(async _ => await ProcessBatchOfMessages(cancellationToken).ConfigureAwait(false), null, MessageHandlingPeriod.Value, MessageHandlingPeriod.Value);
            }

            var consumer = _rabbitMqConnectionFactory.CreateConsumer(Channel);
            consumer.ReceivedAsync += async (_, eventArgs) =>
            {
                lock (_lock)
                {
                    _messages.Add(eventArgs);
                    if (_messages.Count < PrefetchCount)
                    {
                        return;
                    }
                }

                await ProcessBatchOfMessages(cancellationToken).ConfigureAwait(false);
            };

            await Channel.BasicConsumeAsync(queue: QueueName, autoAck: false, consumer: consumer).ConfigureAwait(false);
        }

        private async Task ProcessBatchOfMessages(CancellationToken cancellationToken)
        {
            var messages = GetMessages();
            if (!messages.Any())
            {
                return;
            }

            await ExecutePipeline(messages, cancellationToken).ConfigureAwait(false);
        }

        private async Task ExecutePipeline(IList<BasicDeliverEventArgs> messages, CancellationToken cancellationToken)
        {
            if (!_batchMessageHandlingMiddlewares.Any())
            {
                await Handle(messages, cancellationToken);
                return;
            }

            Func<Task> handleFunction = async () => await Handle(messages, cancellationToken);
            foreach (var middleware in _batchMessageHandlingMiddlewares)
            {
                var previousHandleFunction = handleFunction;
                handleFunction = async () => await middleware.Handle(messages, previousHandleFunction, cancellationToken);
            }

            await handleFunction().ConfigureAwait(false);
        }

        private async Task Handle(IEnumerable<BasicDeliverEventArgs> messages, CancellationToken cancellationToken)
        {
            var messagesCollection = messages.ToList();
            await HandleMessages(messagesCollection, cancellationToken).ConfigureAwait(false);
            var latestDeliveryTag = messagesCollection.Max(x => x.DeliveryTag);
            await Channel.EnsureIsNotNull().BasicAckAsync(latestDeliveryTag, true).ConfigureAwait(false);
        }

        private IList<BasicDeliverEventArgs> GetMessages()
        {
            lock (_lock)
            {
                if (!_messages.Any())
                {
                    return new List<BasicDeliverEventArgs>();
                }

                var messages = _messages.ToList();
                _messages.Clear();
                return messages;
            }
        }

        private void ValidateProperties()
        {
            if (string.IsNullOrEmpty(QueueName))
            {
                throw new BatchMessageHandlerInvalidPropertyValueException("Queue name could not be empty.", nameof(QueueName));
            }

            if (PrefetchCount < 1)
            {
                throw new BatchMessageHandlerInvalidPropertyValueException("PrefetchCount value should be more than one.", nameof(PrefetchCount));
            }
        }

        public abstract Task HandleMessages(IEnumerable<BasicDeliverEventArgs> messages, CancellationToken cancellationToken);

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _timer?.Change(Timeout.Infinite, 0);
            _loggingService.LogInformation($"Batch message handler {GetType()} has been stopped.");
            return Task.CompletedTask;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                _timer?.Dispose();
                Connection?.Dispose();
                Channel?.Dispose();
            }

            _disposed = true;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~BaseBatchMessageHandler()
        {
            Dispose(false);
        }
    }
}
