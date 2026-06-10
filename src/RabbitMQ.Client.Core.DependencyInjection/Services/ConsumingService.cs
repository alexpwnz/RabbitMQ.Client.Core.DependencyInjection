using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.InternalExtensions.Validation;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    public sealed class ConsumingService : IConsumingService, IConsumingServiceDeclaration, IDisposable
    {
        public IConnection? Connection { get; private set; }

        public IChannel? Channel { get; private set; }

        public AsyncEventingBasicConsumer? Consumer { get; private set; }

        private bool _consumingStarted;

        private readonly IMessageHandlingPipelineExecutingService _messageHandlingPipelineExecutingService;
        private readonly IEnumerable<RabbitMqExchange> _exchanges;

        private IEnumerable<string> _consumerTags = new List<string>();

        public ConsumingService(
            IMessageHandlingPipelineExecutingService messageHandlingPipelineExecutingService,
            IEnumerable<RabbitMqExchange> exchanges)
        {
            _messageHandlingPipelineExecutingService = messageHandlingPipelineExecutingService;
            _exchanges = exchanges;
        }

        public void Dispose()
        {
            Channel?.Dispose();
            Connection?.Dispose();
        }

        public void UseConnection(IConnection connection)
        {
            Connection = connection;
        }

        public void UseChannel(IChannel channel)
        {
            Channel = channel;
        }

        public void UseConsumer(AsyncEventingBasicConsumer consumer)
        {
            Consumer = consumer;
        }

        public async Task StartConsumingAsync()
        {
            Channel.EnsureIsNotNull();
            Consumer.EnsureIsNotNull();

            if (_consumingStarted)
            {
                return;
            }

            Consumer.ReceivedAsync += ConsumerOnReceived;
            _consumingStarted = true;

            var consumptionExchanges = _exchanges.Where(x => x.IsConsuming);
            var consumerTags = new List<string>();
            foreach (var exchange in consumptionExchanges)
            {
                foreach (var queue in exchange.Options.Queues)
                {
                    var tag = await Channel.BasicConsumeAsync(queue: queue.Name, autoAck: false, consumer: Consumer).ConfigureAwait(false);
                    consumerTags.Add(tag);
                }
            }
            _consumerTags = consumerTags.Distinct().ToList();
        }

        public async Task StopConsumingAsync()
        {
            Channel.EnsureIsNotNull();
            Consumer.EnsureIsNotNull();

            if (!_consumingStarted)
            {
                return;
            }

            Consumer.ReceivedAsync -= ConsumerOnReceived;
            _consumingStarted = false;
            foreach (var tag in _consumerTags)
            {
                await Channel.BasicCancelAsync(tag).ConfigureAwait(false);
            }
        }

        private Task AckAction(BasicDeliverEventArgs eventArgs) => Channel.EnsureIsNotNull().BasicAckAsync(eventArgs.DeliveryTag, false).AsTask();

        private async Task ConsumerOnReceived(object sender, BasicDeliverEventArgs eventArgs)
        {
            var exchangeOptions = _exchanges.FirstOrDefault(x => string.Equals(x.Name, eventArgs.Exchange)).EnsureIsNotNull().Options;
            var context = new MessageHandlingContext(eventArgs, AckAction, exchangeOptions.DisableAutoAck);
            await _messageHandlingPipelineExecutingService.Execute(context);
        }
    }
}
