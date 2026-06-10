using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        private readonly ConcurrentDictionary<IChannel, ChannelConfirmTracker> _confirmTrackers = new();
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
                        return CreatePooledChannel(channel);
                    }

                    RemoveChannel(channel);
                    _logger.LogWarning("Discarded closed channel from pool. Total channels: {TotalChannels}", _totalChannels);
                }

                var connection = _connection ?? throw new InvalidOperationException(
                    "Producer connection has not been configured. Ensure AddRabbitMqServices or AddRabbitMqProducer was called.");

                IChannel newChannel;
                if (_options.EnablePublisherConfirms)
                {
                    var createOptions = new CreateChannelOptions(true, true, null, null);
                    newChannel = await connection.CreateChannelAsync(createOptions, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    newChannel = await connection.CreateChannelAsync(null, cancellationToken).ConfigureAwait(false);
                }

                newChannel.CallbackExceptionAsync += HandleChannelCallbackException;

                if (_options.EnablePublisherConfirms)
                {
                    SetupConfirmationTracking(newChannel);
                }

                Interlocked.Increment(ref _totalChannels);
                _logger.LogDebug("Created new channel. Total channels: {TotalChannels}", _totalChannels);
                return CreatePooledChannel(newChannel);
            }
            catch
            {
                _semaphore.Release();
                throw;
            }
        }

        internal async Task WaitForConfirmationAsync(IChannel channel, ulong sequenceNumber, TimeSpan timeout)
        {
            if (!_options.EnablePublisherConfirms)
            {
                return;
            }

            if (!_confirmTrackers.TryGetValue(channel, out var tracker))
            {
                return;
            }

            using var cts = new CancellationTokenSource(timeout);
            var confirmed = await tracker.WaitAsync(sequenceNumber, cts.Token).ConfigureAwait(false);
            if (!confirmed)
            {
                throw new InvalidOperationException(
                    $"Broker rejected (nack'd) message with sequence number {sequenceNumber}.");
            }
        }

        internal void Return(IChannel channel)
        {
            if (_disposed || !channel.IsOpen)
            {
                RemoveChannel(channel);
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
                RemoveChannel(channel);
            }

            _semaphore.Dispose();
        }

        private PooledChannel CreatePooledChannel(IChannel channel)
        {
            return new PooledChannel(channel, this, _options.EnablePublisherConfirms);
        }

        private void SetupConfirmationTracking(IChannel channel)
        {
            var tracker = new ChannelConfirmTracker(channel, _logger);
            _confirmTrackers[channel] = tracker;
        }

        private void RemoveChannel(IChannel channel)
        {
            if (_confirmTrackers.TryRemove(channel, out var tracker))
            {
                tracker.Dispose();
            }

            channel.Dispose();
            Interlocked.Decrement(ref _totalChannels);
        }

        private void ClearIdleChannels()
        {
            while (_idleChannels.TryTake(out var channel))
            {
                RemoveChannel(channel);
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

        private sealed class ChannelConfirmTracker : IDisposable
        {
            private readonly IChannel _channel;
            private readonly ILogger _logger;
            private readonly ConcurrentDictionary<ulong, TaskCompletionSource<bool>> _pending = new();
            private readonly object _lock = new();
            private bool _disposed;

            public ChannelConfirmTracker(IChannel channel, ILogger logger)
            {
                _channel = channel;
                _logger = logger;
                _channel.BasicAcksAsync += HandleBasicAcks;
                _channel.BasicNacksAsync += HandleBasicNacks;
            }

            public async Task<bool> WaitAsync(ulong sequenceNumber, CancellationToken cancellationToken)
            {
                var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                _pending[sequenceNumber] = tcs;

                using var registration = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));

                try
                {
                    return await tcs.Task.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    _pending.TryRemove(sequenceNumber, out _);
                    throw;
                }
            }

            private Task HandleBasicAcks(object sender, BasicAckEventArgs @event)
            {
                lock (_lock)
                {
                    if (@event.Multiple)
                    {
                        foreach (var kvp in _pending)
                        {
                            if (kvp.Key <= @event.DeliveryTag)
                            {
                                if (kvp.Value.TrySetResult(true))
                                {
                                    _pending.TryRemove(kvp.Key, out _);
                                }
                            }
                        }
                    }
                    else
                    {
                        if (_pending.TryRemove(@event.DeliveryTag, out var tcs))
                        {
                            tcs.TrySetResult(true);
                        }
                    }
                }

                return Task.CompletedTask;
            }

            private Task HandleBasicNacks(object sender, BasicNackEventArgs @event)
            {
                lock (_lock)
                {
                    if (@event.Multiple)
                    {
                        foreach (var kvp in _pending)
                        {
                            if (kvp.Key <= @event.DeliveryTag)
                            {
                                if (kvp.Value.TrySetResult(false))
                                {
                                    _pending.TryRemove(kvp.Key, out _);
                                }
                            }
                        }
                    }
                    else
                    {
                        if (_pending.TryRemove(@event.DeliveryTag, out var tcs))
                        {
                            tcs.TrySetResult(false);
                        }
                    }
                }

                return Task.CompletedTask;
            }

            public void Dispose()
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                _channel.BasicAcksAsync -= HandleBasicAcks;
                _channel.BasicNacksAsync -= HandleBasicNacks;

                lock (_lock)
                {
                    foreach (var kvp in _pending)
                    {
                        kvp.Value.TrySetException(new ObjectDisposedException(nameof(ChannelConfirmTracker)));
                    }

                    _pending.Clear();
                }
            }
        }
    }
}
