using System;
using System.Threading.Tasks;

namespace RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces
{
    public interface IProducingService
    {
        IConnection? Connection { get; }

        IChannel? Channel { get; }

        Task SendAsync<T>(T @object, string exchangeName, string routingKey) where T : class;

        Task SendAsync<T>(T @object, string exchangeName, string routingKey, int millisecondsDelay) where T : class;

        Task SendJsonAsync(string json, string exchangeName, string routingKey);

        Task SendJsonAsync(string json, string exchangeName, string routingKey, int millisecondsDelay);

        Task SendStringAsync(string message, string exchangeName, string routingKey);

        Task SendStringAsync(string message, string exchangeName, string routingKey, int millisecondsDelay);

        Task SendAsync(ReadOnlyMemory<byte> bytes, IBasicProperties properties, string exchangeName, string routingKey);

        Task SendAsync(ReadOnlyMemory<byte> bytes, IBasicProperties properties, string exchangeName, string routingKey, int millisecondsDelay);
    }
}
