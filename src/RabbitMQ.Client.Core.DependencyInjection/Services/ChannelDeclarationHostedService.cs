using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;

namespace RabbitMQ.Client.Core.DependencyInjection.Services
{
    public class ChannelDeclarationHostedService : IHostedService
    {
        private readonly IChannelDeclarationService _channelDeclarationService;

        public ChannelDeclarationHostedService(IChannelDeclarationService channelDeclarationService)
        {
            _channelDeclarationService = channelDeclarationService;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await _channelDeclarationService.SetConnectionInfrastructureForRabbitMqServicesAsync().ConfigureAwait(false);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
