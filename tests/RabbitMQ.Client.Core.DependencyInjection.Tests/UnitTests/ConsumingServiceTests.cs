using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Moq;
using RabbitMQ.Client.Core.DependencyInjection.MessageHandlers;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Events;
using Xunit;

namespace RabbitMQ.Client.Core.DependencyInjection.Tests.UnitTests
{
    public class ConsumingServiceTests
    {
        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        [InlineData(15)]
        [InlineData(20)]
        [InlineData(25)]
        public async Task ShouldProperlyConsumeMessages(int numberOfMessages)
        {
            var channelMock = new Mock<IChannel>();
            var connectionMock = new Mock<IConnection>();
            var consumer = new AsyncEventingBasicConsumer(channelMock.Object);
            var handlerMock = new Mock<IMessageHandler>();

            channelMock
                .Setup(x => x.BasicConsumeAsync(
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<bool>(),
                    It.IsAny<IDictionary<string, object>>(),
                    It.IsAny<IAsyncBasicConsumer>(),
                    It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync("tag");

            var messageHandlingPipelineExecutingServiceMock = new Mock<IMessageHandlingPipelineExecutingService>();
            var loggingServiceMock = new Mock<ILoggingService>();

            var consumingService = new ConsumingService(messageHandlingPipelineExecutingServiceMock.Object, loggingServiceMock.Object);

            var declaration = (IConsumingServiceDeclaration)consumingService;
            var handlerConsumer = new HandlerConsumerChannel(
                connectionMock.Object,
                channelMock.Object,
                consumer,
                handlerMock.Object,
                "test.queue",
                "exchange",
                new List<string> { "routing.key" });

            declaration.AddHandlerConsumer(handlerConsumer);

            await consumer.HandleBasicDeliverAsync(
                "1",
                0,
                false,
                "exchange",
                "routing.key",
                null,
                new ReadOnlyMemory<byte>(),
                default);

            messageHandlingPipelineExecutingServiceMock.Verify(x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), It.IsAny<IBaseMessageHandler>(), It.IsAny<string>()), Times.Never);

            await consumingService.StartConsumingAsync();

            for (var i = 1; i <= numberOfMessages; i++)
            {
                await consumer.HandleBasicDeliverAsync(
                    "1",
                    (ulong)numberOfMessages,
                    false,
                    "exchange",
                    "routing.key",
                    null,
                    new ReadOnlyMemory<byte>(),
                    default);
            }

            messageHandlingPipelineExecutingServiceMock.Verify(
                x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), handlerMock.Object, It.IsAny<string>()),
                Times.Exactly(numberOfMessages));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        [InlineData(15)]
        [InlineData(20)]
        [InlineData(25)]
        public async Task ShouldProperlyStopConsumingMessages(int numberOfMessages)
        {
            var channelMock = new Mock<IChannel>();
            var connectionMock = new Mock<IConnection>();
            var consumer = new AsyncEventingBasicConsumer(channelMock.Object);
            var handlerMock = new Mock<IMessageHandler>();

            channelMock
                .Setup(x => x.BasicConsumeAsync(
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<bool>(),
                    It.IsAny<IDictionary<string, object>>(),
                    It.IsAny<IAsyncBasicConsumer>(),
                    It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync("tag");

            var messageHandlingPipelineExecutingServiceMock = new Mock<IMessageHandlingPipelineExecutingService>();
            var loggingServiceMock = new Mock<ILoggingService>();

            var consumingService = new ConsumingService(messageHandlingPipelineExecutingServiceMock.Object, loggingServiceMock.Object);

            var declaration = (IConsumingServiceDeclaration)consumingService;
            var handlerConsumer = new HandlerConsumerChannel(
                connectionMock.Object,
                channelMock.Object,
                consumer,
                handlerMock.Object,
                "test.queue",
                "exchange",
                new List<string> { "routing.key" });

            declaration.AddHandlerConsumer(handlerConsumer);

            await consumingService.StartConsumingAsync();

            for (var i = 1; i <= numberOfMessages; i++)
            {
                await consumer.HandleBasicDeliverAsync(
                    "1",
                    (ulong)numberOfMessages,
                    false,
                    "exchange",
                    "routing.key",
                    null,
                    new ReadOnlyMemory<byte>(),
                    default);
            }

            messageHandlingPipelineExecutingServiceMock.Verify(
                x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), handlerMock.Object, It.IsAny<string>()),
                Times.Exactly(numberOfMessages));

            await consumingService.StopConsumingAsync();

            await consumer.HandleBasicDeliverAsync(
                "1",
                0,
                false,
                "exchange",
                "routing.key",
                null,
                new ReadOnlyMemory<byte>(),
                default);

            messageHandlingPipelineExecutingServiceMock.Verify(
                x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), handlerMock.Object, It.IsAny<string>()),
                Times.Exactly(numberOfMessages));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(10)]
        public async Task ShouldProperlyConsumeMessagesForMultipleHandlers(int numberOfMessages)
        {
            var channelMock1 = new Mock<IChannel>();
            var channelMock2 = new Mock<IChannel>();
            var connectionMock = new Mock<IConnection>();
            var consumer1 = new AsyncEventingBasicConsumer(channelMock1.Object);
            var consumer2 = new AsyncEventingBasicConsumer(channelMock2.Object);
            var handlerMock1 = new Mock<IMessageHandler>();
            var handlerMock2 = new Mock<IMessageHandler>();

            channelMock1
                .Setup(x => x.BasicConsumeAsync(
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<bool>(),
                    It.IsAny<IDictionary<string, object>>(),
                    It.IsAny<IAsyncBasicConsumer>(),
                    It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync("tag1");
            channelMock2
                .Setup(x => x.BasicConsumeAsync(
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<bool>(),
                    It.IsAny<IDictionary<string, object>>(),
                    It.IsAny<IAsyncBasicConsumer>(),
                    It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync("tag2");

            var messageHandlingPipelineExecutingServiceMock = new Mock<IMessageHandlingPipelineExecutingService>();
            var loggingServiceMock = new Mock<ILoggingService>();

            var consumingService = new ConsumingService(messageHandlingPipelineExecutingServiceMock.Object, loggingServiceMock.Object);

            var declaration = (IConsumingServiceDeclaration)consumingService;
            declaration.AddHandlerConsumer(new HandlerConsumerChannel(
                connectionMock.Object, channelMock1.Object, consumer1, handlerMock1.Object,
                "queue1", "exchange1", new List<string> { "key1" }));
            declaration.AddHandlerConsumer(new HandlerConsumerChannel(
                connectionMock.Object, channelMock2.Object, consumer2, handlerMock2.Object,
                "queue2", "exchange2", new List<string> { "key2" }));

            await consumingService.StartConsumingAsync();

            for (var i = 1; i <= numberOfMessages; i++)
            {
                await consumer1.HandleBasicDeliverAsync(
                    "1", (ulong)i, false, "exchange1", "key1", null, new ReadOnlyMemory<byte>(), default);
                await consumer2.HandleBasicDeliverAsync(
                    "2", (ulong)i, false, "exchange2", "key2", null, new ReadOnlyMemory<byte>(), default);
            }

            messageHandlingPipelineExecutingServiceMock.Verify(
                x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), handlerMock1.Object, It.IsAny<string>()),
                Times.Exactly(numberOfMessages));
            messageHandlingPipelineExecutingServiceMock.Verify(
                x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), handlerMock2.Object, It.IsAny<string>()),
                Times.Exactly(numberOfMessages));
        }

        [Fact]
        public async Task ShouldNotProcessMessagesBeforeStartConsuming()
        {
            var channelMock = new Mock<IChannel>();
            var connectionMock = new Mock<IConnection>();
            var consumer = new AsyncEventingBasicConsumer(channelMock.Object);
            var handlerMock = new Mock<IMessageHandler>();

            channelMock
                .Setup(x => x.BasicConsumeAsync(
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<bool>(),
                    It.IsAny<IDictionary<string, object>>(),
                    It.IsAny<IAsyncBasicConsumer>(),
                    It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync("tag");

            var messageHandlingPipelineExecutingServiceMock = new Mock<IMessageHandlingPipelineExecutingService>();
            var loggingServiceMock = new Mock<ILoggingService>();

            var consumingService = new ConsumingService(messageHandlingPipelineExecutingServiceMock.Object, loggingServiceMock.Object);

            var declaration = (IConsumingServiceDeclaration)consumingService;
            declaration.AddHandlerConsumer(new HandlerConsumerChannel(
                connectionMock.Object, channelMock.Object, consumer, handlerMock.Object,
                "test.queue", "exchange", new List<string> { "routing.key" }));

            await consumer.HandleBasicDeliverAsync(
                "1", 0, false, "exchange", "routing.key", null, new ReadOnlyMemory<byte>(), default);

            messageHandlingPipelineExecutingServiceMock.Verify(
                x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), handlerMock.Object, It.IsAny<string>()),
                Times.Never);

            await consumingService.StartConsumingAsync();

            messageHandlingPipelineExecutingServiceMock.Verify(
                x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), handlerMock.Object, It.IsAny<string>()),
                Times.Never);
        }

        [Fact]
        public async Task ShouldHandleStartStopStartSequence()
        {
            var channelMock = new Mock<IChannel>();
            var connectionMock = new Mock<IConnection>();
            var consumer = new AsyncEventingBasicConsumer(channelMock.Object);
            var handlerMock = new Mock<IMessageHandler>();

            channelMock
                .Setup(x => x.BasicConsumeAsync(
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<bool>(),
                    It.IsAny<IDictionary<string, object>>(),
                    It.IsAny<IAsyncBasicConsumer>(),
                    It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync("tag");

            var messageHandlingPipelineExecutingServiceMock = new Mock<IMessageHandlingPipelineExecutingService>();
            var loggingServiceMock = new Mock<ILoggingService>();

            var consumingService = new ConsumingService(messageHandlingPipelineExecutingServiceMock.Object, loggingServiceMock.Object);

            var declaration = (IConsumingServiceDeclaration)consumingService;
            declaration.AddHandlerConsumer(new HandlerConsumerChannel(
                connectionMock.Object, channelMock.Object, consumer, handlerMock.Object,
                "test.queue", "exchange", new List<string> { "routing.key" }));

            await consumingService.StartConsumingAsync();

            await consumer.HandleBasicDeliverAsync(
                "1", 1, false, "exchange", "routing.key", null, new ReadOnlyMemory<byte>(), default);

            messageHandlingPipelineExecutingServiceMock.Verify(
                x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), handlerMock.Object, It.IsAny<string>()),
                Times.Once);

            await consumingService.StopConsumingAsync();

            await consumer.HandleBasicDeliverAsync(
                "1", 2, false, "exchange", "routing.key", null, new ReadOnlyMemory<byte>(), default);

            messageHandlingPipelineExecutingServiceMock.Verify(
                x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), handlerMock.Object, It.IsAny<string>()),
                Times.Once);

            await consumingService.StartConsumingAsync();

            await consumer.HandleBasicDeliverAsync(
                "1", 3, false, "exchange", "routing.key", null, new ReadOnlyMemory<byte>(), default);

            messageHandlingPipelineExecutingServiceMock.Verify(
                x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), handlerMock.Object, It.IsAny<string>()),
                Times.Exactly(2));
        }

        [Fact]
        public async Task ShouldNotCrashOnDoubleStart()
        {
            var channelMock = new Mock<IChannel>();
            var connectionMock = new Mock<IConnection>();
            var consumer = new AsyncEventingBasicConsumer(channelMock.Object);
            var handlerMock = new Mock<IMessageHandler>();

            channelMock
                .Setup(x => x.BasicConsumeAsync(
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<bool>(),
                    It.IsAny<IDictionary<string, object>>(),
                    It.IsAny<IAsyncBasicConsumer>(),
                    It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync("tag");

            var messageHandlingPipelineExecutingServiceMock = new Mock<IMessageHandlingPipelineExecutingService>();
            var loggingServiceMock = new Mock<ILoggingService>();

            var consumingService = new ConsumingService(messageHandlingPipelineExecutingServiceMock.Object, loggingServiceMock.Object);

            var declaration = (IConsumingServiceDeclaration)consumingService;
            declaration.AddHandlerConsumer(new HandlerConsumerChannel(
                connectionMock.Object, channelMock.Object, consumer, handlerMock.Object,
                "test.queue", "exchange", new List<string> { "routing.key" }));

            await consumingService.StartConsumingAsync();
            await consumingService.StartConsumingAsync();

            await consumer.HandleBasicDeliverAsync(
                "1", 1, false, "exchange", "routing.key", null, new ReadOnlyMemory<byte>(), default);

            messageHandlingPipelineExecutingServiceMock.Verify(
                x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), handlerMock.Object, It.IsAny<string>()),
                Times.Once);
        }

        [Fact]
        public async Task ShouldNotCrashOnDoubleStop()
        {
            var channelMock = new Mock<IChannel>();
            var connectionMock = new Mock<IConnection>();
            var consumer = new AsyncEventingBasicConsumer(channelMock.Object);
            var handlerMock = new Mock<IMessageHandler>();

            channelMock
                .Setup(x => x.BasicConsumeAsync(
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<bool>(),
                    It.IsAny<IDictionary<string, object>>(),
                    It.IsAny<IAsyncBasicConsumer>(),
                    It.IsAny<System.Threading.CancellationToken>()))
                .ReturnsAsync("tag");

            var messageHandlingPipelineExecutingServiceMock = new Mock<IMessageHandlingPipelineExecutingService>();
            var loggingServiceMock = new Mock<ILoggingService>();

            var consumingService = new ConsumingService(messageHandlingPipelineExecutingServiceMock.Object, loggingServiceMock.Object);

            var declaration = (IConsumingServiceDeclaration)consumingService;
            declaration.AddHandlerConsumer(new HandlerConsumerChannel(
                connectionMock.Object, channelMock.Object, consumer, handlerMock.Object,
                "test.queue", "exchange", new List<string> { "routing.key" }));

            await consumingService.StartConsumingAsync();

            await consumer.HandleBasicDeliverAsync(
                "1", 1, false, "exchange", "routing.key", null, new ReadOnlyMemory<byte>(), default);

            await consumingService.StopConsumingAsync();
            await consumingService.StopConsumingAsync();

            await consumer.HandleBasicDeliverAsync(
                "1", 2, false, "exchange", "routing.key", null, new ReadOnlyMemory<byte>(), default);

            messageHandlingPipelineExecutingServiceMock.Verify(
                x => x.ExecuteForHandler(It.IsAny<MessageHandlingContext>(), handlerMock.Object, It.IsAny<string>()),
                Times.Once);
        }
    }
}
