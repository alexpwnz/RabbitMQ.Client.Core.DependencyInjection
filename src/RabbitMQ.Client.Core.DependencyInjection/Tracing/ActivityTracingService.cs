using System;
using System.Collections.Generic;
using System.Diagnostics;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Tracing
{
    internal sealed class ActivityTracingService : ITracingService, IDisposable
    {
        private readonly ActivitySource _activitySource;
        private readonly ActivityTracingOptions _options;

        public ActivityTracingService(ActivityTracingOptions options)
        {
            _options = options;
            _activitySource = new ActivitySource(options.ActivitySourceName);
        }

        public Activity? StartPublishActivity(IBasicProperties properties, string exchange, string routingKey)
        {
            if (!_options.TracePublishing)
            {
                return null;
            }

            var activity = _activitySource.StartActivity("rabbitmq.publish", ActivityKind.Producer);
            if (activity is null)
            {
                return null;
            }

            activity.SetTag("messaging.system", "rabbitmq");
            activity.SetTag("messaging.destination", exchange);
            activity.SetTag("messaging.destination_kind", "exchange");
            activity.SetTag("messaging.rabbitmq.routing_key", routingKey);

            properties.Headers ??= new Dictionary<string, object?>();
            TraceContextPropagation.Inject(properties.Headers);

            return activity;
        }

        public Activity? StartConsumeActivity(BasicDeliverEventArgs eventArgs)
        {
            if (!_options.TraceConsuming)
            {
                return null;
            }

            var headers = eventArgs.BasicProperties?.Headers;
            var parentContext = TraceContextPropagation.Extract(headers);

            var activity = _activitySource.StartActivity(
                "rabbitmq.process",
                ActivityKind.Consumer,
                parentContext);

            if (activity is null)
            {
                return null;
            }

            activity.SetTag("messaging.system", "rabbitmq");
            activity.SetTag("messaging.destination", eventArgs.Exchange);
            activity.SetTag("messaging.destination_kind", "exchange");
            activity.SetTag("messaging.rabbitmq.routing_key", eventArgs.RoutingKey);

            if (eventArgs.BasicProperties?.MessageId is { } messageId)
            {
                activity.SetTag("messaging.message_id", messageId);
            }

            if (eventArgs.ConsumerTag is { } consumerTag)
            {
                activity.SetTag("messaging.consumer_id", consumerTag);
            }

            return activity;
        }

        public void StopActivity(Activity? activity)
        {
            activity?.Stop();
            activity?.Dispose();
        }

        public void Dispose()
        {
            _activitySource.Dispose();
        }
    }
}
