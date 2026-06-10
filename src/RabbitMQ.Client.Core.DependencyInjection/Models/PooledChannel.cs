using System;
using RabbitMQ.Client.Core.DependencyInjection.Services;

namespace RabbitMQ.Client.Core.DependencyInjection.Models
{
    public sealed class PooledChannel : IDisposable
    {
        public IChannel Channel { get; }

        private readonly ChannelPool _pool;
        private bool _disposed;

        internal PooledChannel(IChannel channel, ChannelPool pool)
        {
            Channel = channel;
            _pool = pool;
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
