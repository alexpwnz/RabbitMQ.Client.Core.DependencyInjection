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
        private readonly IMessageHandlingPipelineExecutingService _messageHandlingPipelineExecutingService;
        private readonly ILoggingService _loggingService;
        private readonly List<HandlerConsumerChannel> _handlerConsumers = new();
        private bool _consumingStarted;
        private readonly List<string> _consumerTags = new();

        public ConsumingService(
            IMessageHandlingPipelineExecutingService messageHandlingPipelineExecutingService,
            ILoggingService loggingService)
        {
            _messageHandlingPipelineExecutingService = messageHandlingPipelineExecutingService;
            _loggingService = loggingService;
        }

        public void Dispose()
        {
            foreach (var hc in _handlerConsumers)
            {
                hc.Dispose();
            }
        }

        internal IReadOnlyList<HandlerConsumerChannel> HandlerConsumers => _handlerConsumers;

        void IConsumingServiceDeclaration.AddHandlerConsumer(HandlerConsumerChannel handlerConsumer)
        {
            _handlerConsumers.Add(handlerConsumer);
        }

        public async Task StartConsumingAsync()
        {
            if (_consumingStarted)
            {
                return;
            }

            _consumingStarted = true;

            foreach (var hc in _handlerConsumers)
            {
                hc.Channel.EnsureIsNotNull();
                hc.Consumer.EnsureIsNotNull();

                hc.ReceivedHandler = (sender, eventArgs) => ConsumerOnReceived(hc, eventArgs);
                hc.Consumer.ReceivedAsync += hc.ReceivedHandler;

                hc.ConsumerTag = await hc.Channel.BasicConsumeAsync(
                    queue: hc.QueueName,
                    autoAck: false,
                    consumer: hc.Consumer).ConfigureAwait(false);

                _consumerTags.Add(hc.ConsumerTag);
                _loggingService.LogInformation($"Started consuming queue \"{hc.QueueName}\" for handler {hc.Handler.GetType().Name} on exchange \"{hc.Exchange}\".");
            }
        }

        public async Task StopConsumingAsync()
        {
            if (!_consumingStarted)
            {
                return;
            }

            _consumingStarted = false;

            for (int i = 0; i < _handlerConsumers.Count; i++)
            {
                var hc = _handlerConsumers[i];
                if (hc.ReceivedHandler is not null)
                {
                    hc.Consumer.ReceivedAsync -= hc.ReceivedHandler;
                    hc.ReceivedHandler = null;
                }
                if (i < _consumerTags.Count)
                {
                    await hc.Channel.BasicCancelAsync(_consumerTags[i]).ConfigureAwait(false);
                }
            }

            _consumerTags.Clear();
        }

        private async Task ConsumerOnReceived(HandlerConsumerChannel handlerConsumer, BasicDeliverEventArgs eventArgs)
        {
            _loggingService.LogInformation($"A new message received with deliveryTag {eventArgs.DeliveryTag} for handler {handlerConsumer.Handler.GetType().Name}.");

            var channel = handlerConsumer.Channel;
            Task AckAction(BasicDeliverEventArgs args) => channel.BasicAckAsync(args.DeliveryTag, false).AsTask();
            Task NackAction(BasicDeliverEventArgs args) => channel.BasicNackAsync(args.DeliveryTag, false, true).AsTask();
            var context = new MessageHandlingContext(eventArgs, AckAction, NackAction, disableAutoAck: false);

            var matchingRoute = FindMatchingRoute(handlerConsumer, eventArgs.RoutingKey);

            await _messageHandlingPipelineExecutingService.ExecuteForHandler(context, handlerConsumer.Handler, matchingRoute).ConfigureAwait(false);

            if (context.AutoAckEnabled && !context.WasRejected)
            {
                await context.AcknowledgeMessage().ConfigureAwait(false);
            }

            _loggingService.LogInformation($"Message processing finished successfully for handler {handlerConsumer.Handler.GetType().Name}.");
        }

        private static string FindMatchingRoute(HandlerConsumerChannel handlerConsumer, string routingKey)
        {
            foreach (var pattern in handlerConsumer.RoutingKeys)
            {
                if (MatchRoute(pattern, routingKey))
                {
                    return pattern;
                }
            }

            return routingKey;
        }

        private static bool MatchRoute(string pattern, string routingKey)
        {
            var patternParts = pattern.Split('.');
            var keyParts = routingKey.Split('.');

            if (patternParts.Length > keyParts.Length && !pattern.Contains("#"))
            {
                return false;
            }

            int pi = 0, ki = 0;
            while (pi < patternParts.Length && ki < keyParts.Length)
            {
                if (patternParts[pi] == "#")
                {
                    if (pi == patternParts.Length - 1)
                    {
                        return true;
                    }

                    while (ki < keyParts.Length)
                    {
                        if (MatchParts(patternParts, pi + 1, keyParts, ki))
                        {
                            return true;
                        }
                        ki++;
                    }
                    return false;
                }

                if (patternParts[pi] != "*" && patternParts[pi] != keyParts[ki])
                {
                    return false;
                }

                pi++;
                ki++;
            }

            return pi == patternParts.Length && ki == keyParts.Length;
        }

        private static bool MatchParts(string[] pattern, int pi, string[] key, int ki)
        {
            while (pi < pattern.Length && ki < key.Length)
            {
                if (pattern[pi] == "#")
                {
                    return MatchRoute(string.Join(".", pattern[pi..]), string.Join(".", key[ki..]));
                }

                if (pattern[pi] != "*" && pattern[pi] != key[ki])
                {
                    return false;
                }

                pi++;
                ki++;
            }

            return pi == pattern.Length && ki == key.Length;
        }
    }
}
