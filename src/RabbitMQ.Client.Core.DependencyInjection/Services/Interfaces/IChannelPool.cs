using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.Models;

namespace RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces
{
    public interface IChannelPool
    {
        Task<PooledChannel> AcquireAsync(CancellationToken cancellationToken = default);
    }
}
