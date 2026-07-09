using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using RabbitMQ.Client.Core.DependencyInjection.Configuration;
using RabbitMQ.Client.Core.DependencyInjection.InternalExtensions.Validation;
using RabbitMQ.Client.Core.DependencyInjection.MessageHandlers;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    public class ChannelDeclarationService : IChannelDeclarationService
    {
        private readonly RabbitMqConnectionOptions _connectionOptions;
        private readonly IProducingService _producingService;
        private readonly IConsumingService _consumingService;
        private readonly IRabbitMqConnectionFactory _rabbitMqConnectionFactory;
        private readonly IEnumerable<RabbitMqExchange> _exchanges;
        private readonly IEnumerable<MessageHandlerRouter> _routers;
        private readonly IEnumerable<MessageHandlerRegistrationOptions> _registrationOptions;
        private readonly IEnumerable<IMessageHandler> _messageHandlers;
        private readonly IEnumerable<IAsyncMessageHandler> _asyncMessageHandlers;
        private readonly ILoggingService _loggingService;
        private readonly IChannelPool _channelPool;

        public ChannelDeclarationService(
            IProducingService producingService,
            IConsumingService consumingService,
            IRabbitMqConnectionFactory rabbitMqConnectionFactory,
            IOptions<RabbitMqConnectionOptions> connectionOptions,
            IEnumerable<RabbitMqExchange> exchanges,
            IEnumerable<MessageHandlerRouter> routers,
            IEnumerable<MessageHandlerRegistrationOptions> registrationOptions,
            IEnumerable<IMessageHandler> messageHandlers,
            IEnumerable<IAsyncMessageHandler> asyncMessageHandlers,
            ILoggingService loggingService,
            IChannelPool channelPool)
        {
            _producingService = producingService;
            _consumingService = consumingService;
            _rabbitMqConnectionFactory = rabbitMqConnectionFactory;
            _connectionOptions = connectionOptions.Value;
            _exchanges = exchanges;
            _routers = routers;
            _registrationOptions = registrationOptions;
            _messageHandlers = messageHandlers;
            _asyncMessageHandlers = asyncMessageHandlers;
            _loggingService = loggingService;
            _channelPool = channelPool;
        }

        public async Task SetConnectionInfrastructureForRabbitMqServicesAsync()
        {
            if (_connectionOptions.ProducerOptions != null)
            {
                var connection = (await CreateConnectionAsync(_connectionOptions.ProducerOptions).ConfigureAwait(false)).EnsureIsNotNull();
                var tempChannel = await CreateChannelAsync(connection).ConfigureAwait(false);
                await StartClientAsync(tempChannel).ConfigureAwait(false);

                var declaration = (IProducingServiceDeclaration)_producingService;
                declaration!.UseConnection(connection);

                if (_channelPool is ChannelPool pool)
                {
                    pool.SetConnection(connection);
                }

                await tempChannel.CloseAsync().ConfigureAwait(false);
                tempChannel.Dispose();
                _loggingService.LogInformation("Producer connection established. Channel pool initialized.");
            }

            if (_connectionOptions.ConsumerOptions != null)
            {
                var connection = (await CreateConnectionAsync(_connectionOptions.ConsumerOptions).ConfigureAwait(false)).EnsureIsNotNull();
                var tempChannel = await CreateChannelAsync(connection).ConfigureAwait(false);
                await StartClientAsync(tempChannel).ConfigureAwait(false);

                var consumerExchanges = _exchanges.Where(x => x.IsConsuming).ToList();
                var exchangeNames = consumerExchanges.Select(x => x.Name).ToHashSet();

                var exchangeRouters = _routers
                    .Where(r => !string.IsNullOrEmpty(r.Exchange) && exchangeNames.Contains(r.Exchange))
                    .ToList();

                var generalRouters = _routers
                    .Where(r => string.IsNullOrEmpty(r.Exchange))
                    .ToList();

                var handlerTypes = exchangeRouters.Select(r => r.Type).Distinct().ToList();
                foreach (var handlerType in handlerTypes)
                {
                    var router = exchangeRouters.First(r => r.Type == handlerType);
                    var handler = ResolveHandler(handlerType);
                    if (handler is null)
                    {
                        continue;
                    }

                    var exchange = router.Exchange!;
                    var routingKeys = router.RoutePatterns.ToList();
                    var queueName = router.QueueName ?? $"{handlerType.FullName}_{exchange}_handler";

                    var exchangeOptions = consumerExchanges.FirstOrDefault(x => x.Name == exchange)?.Options;
                    if (exchangeOptions is null)
                    {
                        continue;
                    }

                    await tempChannel.QueueDeclareAsync(
                        queue: queueName,
                        durable: exchangeOptions.Queues.FirstOrDefault()?.Durable ?? true,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null).ConfigureAwait(false);

                    foreach (var route in routingKeys)
                    {
                        await tempChannel.QueueBindAsync(
                            queue: queueName,
                            exchange: exchange,
                            routingKey: route).ConfigureAwait(false);
                    }

                    var prefetchCount = GetPrefetchCount(handlerType);
                    var channel = await CreateChannelAsync(connection, _connectionOptions.ConsumerOptions, prefetchCount).ConfigureAwait(false);
                    var consumer = _rabbitMqConnectionFactory.CreateConsumer(channel);

                    var handlerConsumer = new HandlerConsumerChannel(
                        connection,
                        channel,
                        consumer,
                        handler,
                        queueName,
                        exchange,
                        routingKeys);

                    ((IConsumingServiceDeclaration)_consumingService).AddHandlerConsumer(handlerConsumer);
                    _loggingService.LogInformation($"Created per-handler channel for {handlerType.Name} on exchange \"{exchange}\" with queue \"{queueName}\".");
                }

                var generalHandlerTypes = generalRouters.Select(r => r.Type).Distinct().ToList();
                foreach (var handlerType in generalHandlerTypes)
                {
                    var router = generalRouters.First(r => r.Type == handlerType);
                    var handler = ResolveHandler(handlerType);
                    if (handler is null)
                    {
                        continue;
                    }

                    var routingKeys = router.RoutePatterns.ToList();

                    foreach (var consumerExchange in consumerExchanges)
                    {
                        var exchangeName = consumerExchange.Name;
                        var exchangeOptions = consumerExchange.Options;
                        var queueName = $"{handlerType.FullName}_{exchangeName}_handler";

                        await tempChannel.QueueDeclareAsync(
                            queue: queueName,
                            durable: exchangeOptions.Queues.FirstOrDefault()?.Durable ?? true,
                            exclusive: false,
                            autoDelete: false,
                            arguments: null).ConfigureAwait(false);

                        foreach (var route in routingKeys)
                        {
                            await tempChannel.QueueBindAsync(
                                queue: queueName,
                                exchange: exchangeName,
                                routingKey: route).ConfigureAwait(false);
                        }

                        var prefetchCount = GetPrefetchCount(handlerType);
                        var channel = await CreateChannelAsync(connection, _connectionOptions.ConsumerOptions, prefetchCount).ConfigureAwait(false);
                        var consumer = _rabbitMqConnectionFactory.CreateConsumer(channel);

                        var handlerConsumer = new HandlerConsumerChannel(
                            connection,
                            channel,
                            consumer,
                            handler,
                            queueName,
                            exchangeName,
                            routingKeys);

                        ((IConsumingServiceDeclaration)_consumingService).AddHandlerConsumer(handlerConsumer);
                        _loggingService.LogInformation($"Created per-handler channel for {handlerType.Name} on exchange \"{exchangeName}\" with queue \"{queueName}\".");
                    }
                }
            }
        }

        private ushort? GetPrefetchCount(Type handlerType) =>
            _registrationOptions.FirstOrDefault(r => r.HandlerType == handlerType)?.PrefetchCount;

        private IBaseMessageHandler? ResolveHandler(Type handlerType)
        {
            var handler = _messageHandlers.FirstOrDefault(h => h.GetType() == handlerType) as IBaseMessageHandler;
            if (handler is not null)
            {
                return handler;
            }

            handler = _asyncMessageHandlers.FirstOrDefault(h => h.GetType() == handlerType) as IBaseMessageHandler;
            return handler;
        }

        private Task<IConnection?> CreateConnectionAsync(RabbitMqServiceOptions options) => _rabbitMqConnectionFactory.CreateRabbitMqConnectionAsync(options);

        private async Task<IChannel> CreateChannelAsync(IConnection connection, RabbitMqServiceOptions? options = null, ushort? prefetchCountOverride = null)
        {
            connection.CallbackExceptionAsync += HandleConnectionCallbackException;
            connection.ConnectionRecoveryErrorAsync += HandleConnectionRecoveryError;

            var channel = await connection.CreateChannelAsync().ConfigureAwait(false);
            channel.CallbackExceptionAsync += HandleChannelCallbackException;

            var prefetchCount = prefetchCountOverride ?? options?.PrefetchCount ?? 0;
            if (prefetchCount > 0)
            {
                await channel.BasicQosAsync(0, prefetchCount, false).ConfigureAwait(false);
            }

            return channel;
        }

        private async Task StartClientAsync(IChannel channel)
        {
            var deadLetterExchanges = _exchanges
                .Select(x => x.Options)
                .Where(x => !string.IsNullOrEmpty(x.DeadLetterExchange))
                .Select(x => new DeadLetterExchange(x.DeadLetterExchange, x.DeadLetterExchangeType))
                .Distinct(new DeadLetterExchangeEqualityComparer())
                .ToList();

            await StartChannelAsync(channel, _exchanges, deadLetterExchanges).ConfigureAwait(false);
        }

        private static async Task StartChannelAsync(IChannel channel, IEnumerable<RabbitMqExchange> exchanges, IEnumerable<DeadLetterExchange> deadLetterExchanges)
        {
            foreach (var exchange in deadLetterExchanges)
            {
                await StartDeadLetterExchangeAsync(channel, exchange).ConfigureAwait(false);
            }

            foreach (var exchange in exchanges)
            {
                await StartExchangeAsync(channel, exchange).ConfigureAwait(false);
            }
        }

        private static Task StartDeadLetterExchangeAsync(IChannel channel, DeadLetterExchange exchange)
        {
            return channel.ExchangeDeclareAsync(
                exchange: exchange.Name,
                type: exchange.Type,
                durable: true,
                autoDelete: false,
                arguments: null);
        }

        private static async Task StartExchangeAsync(IChannel channel, RabbitMqExchange exchange)
        {
            await channel.ExchangeDeclareAsync(
                exchange: exchange.Name,
                type: exchange.Options.Type,
                durable: exchange.Options.Durable,
                autoDelete: exchange.Options.AutoDelete,
                arguments: exchange.Options.Arguments).ConfigureAwait(false);

            foreach (var queue in exchange.Options.Queues)
            {
                await StartQueueAsync(channel, queue, exchange.Name).ConfigureAwait(false);
            }
        }

        private static async Task StartQueueAsync(IChannel channel, RabbitMqQueueOptions queue, string exchangeName)
        {
            await channel.QueueDeclareAsync(
                queue: queue.Name,
                durable: queue.Durable,
                exclusive: queue.Exclusive,
                autoDelete: queue.AutoDelete,
                arguments: queue.Arguments).ConfigureAwait(false);

            if (queue.RoutingKeys.Count > 0)
            {
                foreach (var route in queue.RoutingKeys)
                {
                    await channel.QueueBindAsync(
                        queue: queue.Name,
                        exchange: exchangeName,
                        routingKey: route).ConfigureAwait(false);
                }
            }
            else
            {
                await channel.QueueBindAsync(
                    queue: queue.Name,
                    exchange: exchangeName,
                    routingKey: queue.Name).ConfigureAwait(false);
            }
        }

        private Task HandleConnectionCallbackException(object sender, CallbackExceptionEventArgs? @event)
        {
            if (@event?.Exception is null)
            {
                return Task.CompletedTask;
            }

            _loggingService.LogError(@event.Exception, @event.Exception.Message);
            throw @event.Exception;
        }

        private Task HandleConnectionRecoveryError(object sender, ConnectionRecoveryErrorEventArgs? @event)
        {
            if (@event?.Exception is null)
            {
                return Task.CompletedTask;
            }

            _loggingService.LogError(@event.Exception, @event.Exception.Message);
            throw @event.Exception;
        }

        private Task HandleChannelCallbackException(object sender, CallbackExceptionEventArgs? @event)
        {
            if (@event?.Exception is null)
            {
                return Task.CompletedTask;
            }

            _loggingService.LogError(@event.Exception, @event.Exception.Message);
            return Task.CompletedTask;
        }
    }
}
