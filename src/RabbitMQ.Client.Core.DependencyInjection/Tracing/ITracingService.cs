using System;
using System.Diagnostics;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Tracing
{
    public interface ITracingService
    {
        Activity? StartPublishActivity(IBasicProperties properties, string exchange, string routingKey);

        Activity? StartConsumeActivity(BasicDeliverEventArgs eventArgs);

        void StopActivity(Activity? activity);
    }
}
