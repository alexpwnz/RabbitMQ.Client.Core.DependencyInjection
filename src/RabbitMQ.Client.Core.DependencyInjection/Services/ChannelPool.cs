using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client.Core.DependencyInjection.Configuration;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    public sealed class ChannelPool : IChannelPool, IDisposable
    {
        private IConnection? _connection;
        private readonly ChannelPoolOptions _options;
        private readonly ConcurrentBag<IChannel> _idleChannels = new();
        private readonly SemaphoreSlim _semaphore;
        private int _totalChannels;
        private bool _disposed;
        private readonly ILogger<ChannelPool> _logger;

        public ChannelPool(IOptions<ChannelPoolOptions> options, ILogger<ChannelPool> logger)
        {
            _options = options.Value;
            _semaphore = new SemaphoreSlim(_options.MaxChannels, _options.MaxChannels);
            _logger = logger;
        }

        public void SetConnection(IConnection connection)
        {
            _connection = connection;
            connection.ConnectionRecoveryErrorAsync += HandleConnectionRecoveryError;
            connection.CallbackExceptionAsync += HandleConnectionCallbackException;
        }

        public async Task<PooledChannel> AcquireAsync(CancellationToken cancellationToken = default)
        {
            await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                while (_idleChannels.TryTake(out var channel))
                {
                    if (channel.IsOpen)
                    {
                        _logger.LogDebug("Reusing idle channel from pool. Total channels: {TotalChannels}", _totalChannels);
                        return new PooledChannel(channel, this);
                    }

                    channel.Dispose();
                    Interlocked.Decrement(ref _totalChannels);
                    _logger.LogWarning("Discarded closed channel from pool. Total channels: {TotalChannels}", _totalChannels);
                }

                var connection = _connection ?? throw new InvalidOperationException(
                    "Producer connection has not been configured. Ensure AddRabbitMqServices or AddRabbitMqProducer was called.");
                var newChannel = await connection.CreateChannelAsync().ConfigureAwait(false);

                newChannel.CallbackExceptionAsync += HandleChannelCallbackException;

                Interlocked.Increment(ref _totalChannels);
                _logger.LogDebug("Created new channel. Total channels: {TotalChannels}", _totalChannels);
                return new PooledChannel(newChannel, this);
            }
            catch
            {
                _semaphore.Release();
                throw;
            }
        }

        internal void Return(IChannel channel)
        {
            if (_disposed || !channel.IsOpen)
            {
                channel.Dispose();
                Interlocked.Decrement(ref _totalChannels);
                _logger.LogDebug("Disposed returned channel. Total channels: {TotalChannels}", _totalChannels);
            }
            else
            {
                _idleChannels.Add(channel);
                _logger.LogDebug("Returned channel to pool. Idle channels: ~{IdleCount}", _idleChannels.Count);
            }

            _semaphore.Release();
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            while (_idleChannels.TryTake(out var channel))
            {
                channel.Dispose();
                Interlocked.Decrement(ref _totalChannels);
            }

            _semaphore.Dispose();
        }

        private void ClearIdleChannels()
        {
            while (_idleChannels.TryTake(out var channel))
            {
                channel.Dispose();
                Interlocked.Decrement(ref _totalChannels);
            }
        }

        private Task HandleConnectionRecoveryError(object sender, ConnectionRecoveryErrorEventArgs @event)
        {
            if (@event.Exception is not null)
            {
                _logger.LogError(@event.Exception, "Connection recovery error in channel pool");
            }

            return Task.CompletedTask;
        }

        private Task HandleConnectionCallbackException(object sender, CallbackExceptionEventArgs @event)
        {
            if (@event.Exception is not null)
            {
                _logger.LogError(@event.Exception, "Connection callback exception in channel pool");
            }

            return Task.CompletedTask;
        }

        private Task HandleChannelCallbackException(object sender, CallbackExceptionEventArgs @event)
        {
            if (@event.Exception is not null)
            {
                _logger.LogError(@event.Exception, "Channel callback exception in pool");
            }

            return Task.CompletedTask;
        }
    }
}
