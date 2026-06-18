using System.Diagnostics;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Tracing
{
    internal sealed class NullTracingService : ITracingService
    {
        public Activity? StartPublishActivity(IBasicProperties properties, string exchange, string routingKey) => null;

        public Activity? StartConsumeActivity(BasicDeliverEventArgs eventArgs) => null;

        public void StopActivity(Activity? activity)
        {
        }
    }
}
