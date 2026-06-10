using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using RabbitMQ.Client.Core.DependencyInjection.Configuration;
using RabbitMQ.Client.Core.DependencyInjection.InternalExtensions.Validation;
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
        private readonly ILoggingService _loggingService;

        public ChannelDeclarationService(
            IProducingService producingService,
            IConsumingService consumingService,
            IRabbitMqConnectionFactory rabbitMqConnectionFactory,
            IOptions<RabbitMqConnectionOptions> connectionOptions,
            IEnumerable<RabbitMqExchange> exchanges,
            ILoggingService loggingService)
        {
            _producingService = producingService;
            _consumingService = consumingService;
            _rabbitMqConnectionFactory = rabbitMqConnectionFactory;
            _connectionOptions = connectionOptions.Value;
            _exchanges = exchanges;
            _loggingService = loggingService;
        }

        public async Task SetConnectionInfrastructureForRabbitMqServicesAsync()
        {
            if (_connectionOptions.ProducerOptions != null)
            {
                var connection = (await CreateConnectionAsync(_connectionOptions.ProducerOptions).ConfigureAwait(false)).EnsureIsNotNull();
                var channel = await CreateChannelAsync(connection).ConfigureAwait(false);
                await StartClientAsync(channel).ConfigureAwait(false);
                var declaration = (IProducingServiceDeclaration)_producingService;
                declaration!.UseConnection(connection);
                declaration.UseChannel(channel);
            }

            if (_connectionOptions.ConsumerOptions != null)
            {
                var connection = (await CreateConnectionAsync(_connectionOptions.ConsumerOptions).ConfigureAwait(false)).EnsureIsNotNull();
                var channel = await CreateChannelAsync(connection).ConfigureAwait(false);
                await StartClientAsync(channel).ConfigureAwait(false);
                var consumer = _rabbitMqConnectionFactory.CreateConsumer(channel);
                var declaration = (IConsumingServiceDeclaration)_consumingService;
                declaration.UseConnection(connection);
                declaration.UseChannel(channel);
                declaration.UseConsumer(consumer);
            }
        }

        private Task<IConnection?> CreateConnectionAsync(RabbitMqServiceOptions options) => _rabbitMqConnectionFactory.CreateRabbitMqConnectionAsync(options);

        private async Task<IChannel> CreateChannelAsync(IConnection connection)
        {
            connection.CallbackExceptionAsync += HandleConnectionCallbackException;
            connection.ConnectionRecoveryErrorAsync += HandleConnectionRecoveryError;

            var channel = await connection.CreateChannelAsync().ConfigureAwait(false);
            channel.CallbackExceptionAsync += HandleChannelCallbackException;
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
