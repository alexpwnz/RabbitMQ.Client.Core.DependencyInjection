using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces
{
    public interface IConsumingService
    {
        IConnection? Connection { get; }

        IChannel? Channel { get; }

        AsyncEventingBasicConsumer? Consumer { get; }

        Task StartConsumingAsync();

        Task StopConsumingAsync();
    }
}
