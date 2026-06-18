using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    public class ConsumptionStarterHostedService : IHostedService
    {
        private readonly IConsumingService _consumingService;

        public ConsumptionStarterHostedService(IConsumingService consumingService)
        {
            _consumingService = consumingService;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await _consumingService.StartConsumingAsync().ConfigureAwait(false);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
