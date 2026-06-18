using System.Threading.Tasks;

namespace RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces
{
    public interface IConsumingService
    {
        Task StartConsumingAsync();

        Task StopConsumingAsync();
    }
}
