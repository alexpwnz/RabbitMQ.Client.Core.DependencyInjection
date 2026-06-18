using System;
using RabbitMQ.Client.Core.DependencyInjection.Services;

namespace RabbitMQ.Client.Core.DependencyInjection.Models
{
    public sealed class PooledChannel : IDisposable
    {
        public IChannel Channel { get; }

        public bool PublisherConfirmsEnabled { get; }

        private readonly ChannelPool _pool;
        private bool _disposed;

        internal PooledChannel(IChannel channel, ChannelPool pool, bool publisherConfirmsEnabled = false)
        {
            Channel = channel;
            _pool = pool;
            PublisherConfirmsEnabled = publisherConfirmsEnabled;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            _pool.Return(Channel);
        }
    }
}
