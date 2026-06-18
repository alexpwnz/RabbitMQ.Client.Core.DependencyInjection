using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.Configuration;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces
{
    public interface IRabbitMqConnectionFactory
    {
        Task<IConnection?> CreateRabbitMqConnectionAsync(RabbitMqServiceOptions? options);

        AsyncEventingBasicConsumer CreateConsumer(IChannel channel);
    }
}
