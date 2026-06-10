using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.Client.Core.DependencyInjection.Configuration;
using RabbitMQ.Client.Core.DependencyInjection.Exceptions;
using RabbitMQ.Client.Core.DependencyInjection.Services;
using RabbitMQ.Client.Core.DependencyInjection.Tests.Fixtures;
using Xunit;

namespace RabbitMQ.Client.Core.DependencyInjection.Tests.IntegrationTests
{
    public class RabbitMqConnectionFactoryTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;

        public RabbitMqConnectionFactoryTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        public async Task ShouldProperlyRetryCreatingInitialConnection(int retries)
        {
            var connectionOptions = new RabbitMqServiceOptions
            {
                HostName = "anotherHost",
                InitialConnectionRetries = retries,
                InitialConnectionRetryTimeoutMilliseconds = 20
            };
            await ExecuteUnsuccessfulConnectionCreationAndAssertResults(connectionOptions);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        public async Task ShouldProperlyRetryCreatingInitialConnectionWithConnectionName(int retries)
        {
            var connectionOptions = new RabbitMqServiceOptions
            {
                HostName = "anotherHost",
                ClientProvidedName = "connectionName",
                InitialConnectionRetries = retries,
                InitialConnectionRetryTimeoutMilliseconds = 20
            };
            await ExecuteUnsuccessfulConnectionCreationAndAssertResults(connectionOptions);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        public async Task ShouldProperlyRetryCreatingInitialConnectionWithTcpEndpoints(int retries)
        {
            var connectionOptions = new RabbitMqServiceOptions
            {
                TcpEndpoints = new List<RabbitMqTcpEndpoint>
                {
                    new()
                    {
                        HostName = "anotherHost"
                    }
                },
                InitialConnectionRetries = retries,
                InitialConnectionRetryTimeoutMilliseconds = 20
            };
            await ExecuteUnsuccessfulConnectionCreationAndAssertResults(connectionOptions);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        public async Task ShouldProperlyRetryCreatingInitialConnectionWithHostNames(int retries)
        {
            var connectionOptions = new RabbitMqServiceOptions
            {
                HostNames = new List<string> { "anotherHost" },
                InitialConnectionRetries = retries,
                InitialConnectionRetryTimeoutMilliseconds = 20
            };
            await ExecuteUnsuccessfulConnectionCreationAndAssertResults(connectionOptions);
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        public async Task ShouldProperlyRetryCreatingInitialConnectionWithHostNamesAndNamedConnection(int retries)
        {
            var connectionOptions = new RabbitMqServiceOptions
            {
                HostNames = new List<string> { "anotherHost" },
                ClientProvidedName = "connectionName",
                InitialConnectionRetries = retries,
                InitialConnectionRetryTimeoutMilliseconds = 20
            };
            await ExecuteUnsuccessfulConnectionCreationAndAssertResults(connectionOptions);
        }

        [Fact]
        public async Task ShouldProperlyCreateInitialConnection()
        {
            var connectionOptions = new RabbitMqServiceOptions
            {
                HostName = _fixture.Hostname,
                Port = _fixture.Port,
                UserName = _fixture.Username,
                Password = _fixture.Password,
                InitialConnectionRetries = 1,
                InitialConnectionRetryTimeoutMilliseconds = 20
            };
            await ExecuteSuccessfulConnectionCreationAndAssertResults(connectionOptions);
        }

        [Fact]
        public async Task ShouldProperlyCreateInitialConnectionWithConnectionName()
        {
            var connectionOptions = new RabbitMqServiceOptions
            {
                HostName = _fixture.Hostname,
                Port = _fixture.Port,
                UserName = _fixture.Username,
                Password = _fixture.Password,
                ClientProvidedName = "connectionName",
                InitialConnectionRetries = 3,
                InitialConnectionRetryTimeoutMilliseconds = 20
            };
            await ExecuteSuccessfulConnectionCreationAndAssertResults(connectionOptions);
        }

        [Fact]
        public async Task ShouldProperlyCreateInitialConnectionWithTcpEndpoints()
        {
            var connectionOptions = new RabbitMqServiceOptions
            {
                TcpEndpoints = new List<RabbitMqTcpEndpoint>
                {
                    new()
                    {
                        HostName = _fixture.Hostname,
                        Port = _fixture.Port
                    }
                },
                InitialConnectionRetries = 3,
                InitialConnectionRetryTimeoutMilliseconds = 20
            };
            await ExecuteSuccessfulConnectionCreationAndAssertResults(connectionOptions);
        }

        [Fact]
        public async Task ShouldProperlyCreateInitialConnectionWithHostNames()
        {
            var connectionOptions = new RabbitMqServiceOptions
            {
                HostNames = new List<string> { _fixture.Hostname },
                Port = _fixture.Port,
                UserName = _fixture.Username,
                Password = _fixture.Password,
                InitialConnectionRetries = 3,
                InitialConnectionRetryTimeoutMilliseconds = 20
            };
            await ExecuteSuccessfulConnectionCreationAndAssertResults(connectionOptions);
        }

        [Fact]
        public async Task ShouldProperlyCreateInitialConnectionWithHostNamesAndNamedConnection()
        {
            var connectionOptions = new RabbitMqServiceOptions
            {
                HostNames = new List<string> { _fixture.Hostname },
                Port = _fixture.Port,
                UserName = _fixture.Username,
                Password = _fixture.Password,
                ClientProvidedName = "connectionName",
                InitialConnectionRetries = 3,
                InitialConnectionRetryTimeoutMilliseconds = 20
            };
            await ExecuteSuccessfulConnectionCreationAndAssertResults(connectionOptions);
        }

        private static async Task ExecuteUnsuccessfulConnectionCreationAndAssertResults(RabbitMqServiceOptions connectionOptions)
        {
            var connectionFactory = new RabbitMqConnectionFactory();
            var exception = await Assert.ThrowsAsync<InitialConnectionException>(() => connectionFactory.CreateRabbitMqConnectionAsync(connectionOptions));
            Assert.Equal(connectionOptions.InitialConnectionRetries, exception.NumberOfRetries);
        }

        private static async Task ExecuteSuccessfulConnectionCreationAndAssertResults(RabbitMqServiceOptions connectionOptions)
        {
            var connectionFactory = new RabbitMqConnectionFactory();
            await using var connection = await connectionFactory.CreateRabbitMqConnectionAsync(connectionOptions);
            Assert.True(connection.IsOpen);
        }
    }
}
