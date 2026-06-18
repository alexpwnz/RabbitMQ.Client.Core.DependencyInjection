using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using RabbitMQ.Client.Core.DependencyInjection.Configuration;
using RabbitMQ.Client.Core.DependencyInjection.InternalExtensions.Validation;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Core.DependencyInjection.Tests.Fixtures;
using RabbitMQ.Client.Core.DependencyInjection.Tests.Stubs;
using Xunit;

namespace RabbitMQ.Client.Core.DependencyInjection.Tests.IntegrationTests
{
    [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
    public class RabbitMqServicesTests : IClassFixture<RabbitMqContainerFixture>
    {
        private readonly RabbitMqContainerFixture _fixture;
        private readonly TimeSpan _globalTestsTimeout = TimeSpan.FromSeconds(60);

        private const string DefaultExchangeName = "exchange.name";
        private const string FirstRoutingKey = "first.routing.key";
        private const string SecondRoutingKey = "second.routing.key";
        private const int RequeueAttempts = 4;

        public RabbitMqServicesTests(RabbitMqContainerFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task ShouldProperlyPublishAndConsumeMessages()
        {
            var callerMock = new Mock<IStubCaller>();
            var serviceCollection = new ServiceCollection();
            serviceCollection
                .AddSingleton(callerMock.Object)
                .AddRabbitMqServices(_fixture.CreateServiceOptions())
                .AddExchange(DefaultExchangeName, GetExchangeOptions())
                .AddMessageHandlerTransient<StubMessageHandler>(FirstRoutingKey)
                .AddAsyncMessageHandlerTransient<StubAsyncMessageHandler>(SecondRoutingKey);

            await using var serviceProvider = serviceCollection.BuildServiceProvider();
            var consumingService = serviceProvider.GetRequiredService<IConsumingService>();
            var producingService = serviceProvider.GetRequiredService<IProducingService>();
            var channelDeclarationService = serviceProvider.GetRequiredService<IChannelDeclarationService>();

            await channelDeclarationService.SetConnectionInfrastructureForRabbitMqServicesAsync();
            await consumingService.StartConsumingAsync();

            await producingService.SendAsync(new { Message = "message" }, DefaultExchangeName, FirstRoutingKey);
            await Task.Delay(2000);
            callerMock.Verify(x => x.Call(It.IsAny<string>()), Times.Once);

            await producingService.SendAsync(new { Message = "message" }, DefaultExchangeName, SecondRoutingKey);
            await Task.Delay(2000);
            callerMock.Verify(x => x.CallAsync(It.IsAny<string>()), Times.Once);
        }

        [Fact]
        public async Task ShouldProperlyRequeueMessages()
        {
            var callerMock = new Mock<IStubCaller>();
            var serviceCollection = new ServiceCollection();
            serviceCollection
                .AddSingleton(callerMock.Object)
                .AddRabbitMqServices(_fixture.CreateServiceOptions())
                .AddExchange(DefaultExchangeName, GetExchangeOptions())
                .AddMessageHandlerTransient<StubExceptionMessageHandler>(FirstRoutingKey);

            await using var serviceProvider = serviceCollection.BuildServiceProvider();
            var consumingService = serviceProvider.GetRequiredService<IConsumingService>();
            var producingService = serviceProvider.GetRequiredService<IProducingService>();
            var channelDeclarationService = serviceProvider.GetRequiredService<IChannelDeclarationService>();

            await channelDeclarationService.SetConnectionInfrastructureForRabbitMqServicesAsync();
            await consumingService.StartConsumingAsync();

            await producingService.SendAsync(new { Message = "message" }, DefaultExchangeName, FirstRoutingKey);

            await Task.Delay(5000);
            callerMock.Verify(x => x.Call(It.IsAny<string>()), Times.Exactly(RequeueAttempts + 1));
        }

        private static RabbitMqExchangeOptions GetExchangeOptions() =>
            new()
            {
                Type = "direct",
                DeadLetterExchange = "exchange.dlx",
                RequeueAttempts = RequeueAttempts,
                RequeueTimeoutMilliseconds = 50,
                Queues = new List<RabbitMqQueueOptions>
                {
                    new()
                    {
                        Name = "test.queue",
                        RoutingKeys = new HashSet<string> { FirstRoutingKey, SecondRoutingKey }
                    }
                }
            };
    }
}
