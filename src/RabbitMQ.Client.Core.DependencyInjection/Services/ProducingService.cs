using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;
using RabbitMQ.Client.Core.DependencyInjection.Exceptions;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Core.DependencyInjection.Tracing;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    public sealed class ProducingService : IProducingService, IProducingServiceDeclaration, IDisposable
    {
        public IConnection? Connection { get; private set; }

        private readonly IChannelPool _channelPool;
        private readonly IReadOnlyCollection<RabbitMqExchange> _exchanges;
        private readonly ITracingService _tracingService;

        private const int QueueExpirationTime = 60000;

        public ProducingService(
            IEnumerable<RabbitMqExchange> exchanges,
            IChannelPool channelPool,
            ITracingService? tracingService = null)
        {
            _exchanges = exchanges.Where(x => x.IsProducing).ToList();
            _channelPool = channelPool;
            _tracingService = tracingService ?? new NullTracingService();
        }

        public void Dispose()
        {
            Connection?.Dispose();
        }

        public void UseConnection(IConnection connection)
        {
            Connection = connection;
        }

        public async Task SendAsync<T>(T @object, string exchangeName, string routingKey) where T : class
        {
            ValidateArguments(exchangeName, routingKey);
            var json = JsonSerializer.Serialize(@object);
            var bytes = Encoding.UTF8.GetBytes(json);
            var properties = CreateJsonProperties();
            await SendAsync(bytes, properties, exchangeName, routingKey).ConfigureAwait(false);
        }

        public async Task SendAsync<T>(T @object, string exchangeName, string routingKey, int millisecondsDelay) where T : class
        {
            ValidateArguments(exchangeName, routingKey);
            var deadLetterExchange = GetDeadLetterExchange(exchangeName);
            var delayedQueueName = await DeclareDelayedQueueAsync(exchangeName, deadLetterExchange, routingKey, millisecondsDelay).ConfigureAwait(false);
            await SendAsync(@object, deadLetterExchange, delayedQueueName).ConfigureAwait(false);
        }

        public async Task SendJsonAsync(string json, string exchangeName, string routingKey)
        {
            ValidateArguments(exchangeName, routingKey);
            var bytes = Encoding.UTF8.GetBytes(json);
            var properties = CreateJsonProperties();
            await SendAsync(bytes, properties, exchangeName, routingKey).ConfigureAwait(false);
        }

        public async Task SendJsonAsync(string json, string exchangeName, string routingKey, int millisecondsDelay)
        {
            ValidateArguments(exchangeName, routingKey);
            var deadLetterExchange = GetDeadLetterExchange(exchangeName);
            var delayedQueueName = await DeclareDelayedQueueAsync(exchangeName, deadLetterExchange, routingKey, millisecondsDelay).ConfigureAwait(false);
            await SendJsonAsync(json, deadLetterExchange, delayedQueueName).ConfigureAwait(false);
        }

        public async Task SendStringAsync(string message, string exchangeName, string routingKey)
        {
            ValidateArguments(exchangeName, routingKey);
            var bytes = Encoding.UTF8.GetBytes(message);
            await SendAsync(bytes, CreateProperties(), exchangeName, routingKey).ConfigureAwait(false);
        }

        public async Task SendStringAsync(string message, string exchangeName, string routingKey, int millisecondsDelay)
        {
            ValidateArguments(exchangeName, routingKey);
            var deadLetterExchange = GetDeadLetterExchange(exchangeName);
            var delayedQueueName = await DeclareDelayedQueueAsync(exchangeName, deadLetterExchange, routingKey, millisecondsDelay).ConfigureAwait(false);
            await SendStringAsync(message, deadLetterExchange, delayedQueueName).ConfigureAwait(false);
        }

        public async Task SendAsync(ReadOnlyMemory<byte> bytes, IBasicProperties properties, string exchangeName, string routingKey)
        {
            ValidateArguments(exchangeName, routingKey);
            using var pooledChannel = await _channelPool.AcquireAsync().ConfigureAwait(false);
            var activity = _tracingService.StartPublishActivity(properties, exchangeName, routingKey);
            try
            {
                await pooledChannel.Channel.BasicPublishAsync(
                    exchange: exchangeName,
                    routingKey: routingKey,
                    mandatory: false,
                    basicProperties: (BasicProperties)properties,
                    body: bytes).ConfigureAwait(false);
            }
            finally
            {
                _tracingService.StopActivity(activity);
            }
        }

        public async Task SendAsync(ReadOnlyMemory<byte> bytes, IBasicProperties properties, string exchangeName, string routingKey, int millisecondsDelay)
        {
            ValidateArguments(exchangeName, routingKey);
            var deadLetterExchange = GetDeadLetterExchange(exchangeName);
            var delayedQueueName = await DeclareDelayedQueueAsync(exchangeName, deadLetterExchange, routingKey, millisecondsDelay).ConfigureAwait(false);
            await SendAsync(bytes, properties, deadLetterExchange, delayedQueueName).ConfigureAwait(false);
        }

        private BasicProperties CreateProperties()
        {
            var properties = new BasicProperties();
            properties.Persistent = true;
            return properties;
        }

        private BasicProperties CreateJsonProperties()
        {
            var properties = new BasicProperties();
            properties.Persistent = true;
            properties.ContentType = "application/json";
            return properties;
        }

        internal void ValidateArguments(string exchangeName, string routingKey)
        {
            if (string.IsNullOrEmpty(exchangeName))
            {
                throw new ArgumentException($"Argument {nameof(exchangeName)} is null or empty.", nameof(exchangeName));
            }
            if (string.IsNullOrEmpty(routingKey))
            {
                throw new ArgumentException($"Argument {nameof(routingKey)} is null or empty.", nameof(routingKey));
            }

            var deadLetterExchanges = _exchanges.Select(x => x.Options.DeadLetterExchange).Distinct();
            if (!_exchanges.Any(x => x.Name == exchangeName) && !deadLetterExchanges.Any(x => x == exchangeName))
            {
                throw new ArgumentException($"Exchange {nameof(exchangeName)} has not been declared yet.", nameof(exchangeName));
            }
        }

        private string GetDeadLetterExchange(string exchangeName)
        {
            var exchange = _exchanges.FirstOrDefault(x => x.Name == exchangeName);
            if (string.IsNullOrEmpty(exchange?.Options.DeadLetterExchange))
            {
                throw new ArgumentException($"Exchange {nameof(exchangeName)} has not been configured with a dead letter exchange.", nameof(exchangeName));
            }

            return exchange.Options.DeadLetterExchange;
        }

        private async Task<string> DeclareDelayedQueueAsync(string exchange, string deadLetterExchange, string routingKey, int millisecondsDelay)
        {
            var delayedQueueName = $"{routingKey}.delayed.{millisecondsDelay}";
            var arguments = CreateArguments(exchange, routingKey, millisecondsDelay);

            using var pooledChannel = await _channelPool.AcquireAsync().ConfigureAwait(false);
            var channel = pooledChannel.Channel;

            await channel.QueueDeclareAsync(
                queue: delayedQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: arguments).ConfigureAwait(false);

            await channel.QueueBindAsync(
                queue: delayedQueueName,
                exchange: deadLetterExchange,
                routingKey: delayedQueueName).ConfigureAwait(false);
            return delayedQueueName;
        }

        private static Dictionary<string, object?> CreateArguments(string exchangeName, string routingKey, int millisecondsDelay) =>
            new Dictionary<string, object?>
            {
                { "x-dead-letter-exchange", exchangeName },
                { "x-dead-letter-routing-key", routingKey },
                { "x-message-ttl", millisecondsDelay },
                { "x-expires", millisecondsDelay + QueueExpirationTime }
            };
    }
}
