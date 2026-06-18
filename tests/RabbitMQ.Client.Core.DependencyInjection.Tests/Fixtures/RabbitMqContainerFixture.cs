using System;
using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.Configuration;
using Testcontainers.RabbitMq;
using Xunit;

namespace RabbitMQ.Client.Core.DependencyInjection.Tests.Fixtures
{
    public class RabbitMqContainerFixture : IAsyncLifetime
    {
        private readonly RabbitMqContainer _container;

        public string Hostname => _container.Hostname;

        public int Port => _container.GetMappedPublicPort(5672);

        public string Username { get; } = "guest";

        public string Password { get; } = "guest";

        public RabbitMqContainerFixture()
        {
            _container = new RabbitMqBuilder()
                .WithUsername(Username)
                .WithPassword(Password)
                .Build();
        }

        public RabbitMqServiceOptions CreateServiceOptions()
        {
            return new RabbitMqServiceOptions
            {
                HostName = Hostname,
                Port = Port,
                UserName = Username,
                Password = Password,
                VirtualHost = "/"
            };
        }

        public async Task InitializeAsync()
        {
            await _container.StartAsync();
        }

        public async Task DisposeAsync()
        {
            await _container.DisposeAsync();
        }
    }
}