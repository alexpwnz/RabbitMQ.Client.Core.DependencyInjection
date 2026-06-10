using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.MessageHandlers;
using RabbitMQ.Client.Core.DependencyInjection.Models;

namespace RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces
{
    public interface IMessageHandlingPipelineExecutingService
    {
        Task Execute(MessageHandlingContext context);

        Task ExecuteForHandler(MessageHandlingContext context, IBaseMessageHandler handler, string matchingRoute);
    }
}
