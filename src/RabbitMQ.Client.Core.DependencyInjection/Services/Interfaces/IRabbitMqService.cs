namespace RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces
{
    internal interface IRabbitMqService
    {
        void UseConnection(IConnection connection);

        void UseChannel(IChannel channel);
    }
}
