using System;

namespace RabbitMQ.Client.Core.DependencyInjection.Configuration
{
    public class ChannelPoolOptions
    {
        public int MinChannels { get; set; } = 5;

        public int MaxChannels { get; set; } = 30;

        public TimeSpan ChannelIdleTimeout { get; set; } = TimeSpan.FromMinutes(2);

        public bool EnablePublisherConfirms { get; set; }

        public TimeSpan PublisherConfirmationTimeout { get; set; } = TimeSpan.FromSeconds(5);
    }
}
