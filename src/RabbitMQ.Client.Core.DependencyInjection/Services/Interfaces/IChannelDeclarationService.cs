using System.Threading.Tasks;

namespace RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces
{
    public interface IChannelDeclarationService
    {
        Task SetConnectionInfrastructureForRabbitMqServicesAsync();
    }
}
