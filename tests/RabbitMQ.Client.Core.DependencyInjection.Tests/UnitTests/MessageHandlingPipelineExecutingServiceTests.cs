using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Moq;
using RabbitMQ.Client.Core.DependencyInjection.MessageHandlers;
using RabbitMQ.Client.Core.DependencyInjection.Middlewares;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Core.DependencyInjection.Services;
using RabbitMQ.Client.Core.DependencyInjection.Services.Interfaces;
using RabbitMQ.Client.Core.DependencyInjection.Tests.Stubs;
using RabbitMQ.Client.Events;
using Xunit;
using RabbitMQ.Client;

namespace RabbitMQ.Client.Core.DependencyInjection.Tests.UnitTests
{
    public class MessageHandlingPipelineExecutingServiceTests
    {
        [Fact]
        public async Task ShouldProperlyExecutePipelineWithNoAdditionalMiddlewares()
        {
            var propertiesMock = new Mock<IReadOnlyBasicProperties>();
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routingKey", propertiesMock.Object, new byte[0]);
            var messageHandlingServiceMock = new Mock<IMessageHandlingService>();
            var errorProcessingServiceMock = new Mock<IErrorProcessingService>();

            var service = CreateService(
                messageHandlingServiceMock.Object,
                errorProcessingServiceMock.Object,
                Enumerable.Empty<IMessageHandlingMiddleware>());

            var context = new MessageHandlingContext(eventArgs, AckAction, false);
            await service.Execute(context);
            messageHandlingServiceMock.Verify(x => x.HandleMessageReceivingEvent(It.IsAny<MessageHandlingContext>()), Times.Once);
        }

        [Fact]
        public async Task ShouldProperlyExecutePipeline()
        {
            var propertiesMock = new Mock<IReadOnlyBasicProperties>();
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routingKey", propertiesMock.Object, new byte[0]);
            var messageHandlingServiceMock = new Mock<IMessageHandlingService>();
            var errorProcessingServiceMock = new Mock<IErrorProcessingService>();

            var middlewareOrderingMap = new Dictionary<int, int>();
            var firstMiddleware = new StubMessageHandlingMiddleware(1, middlewareOrderingMap, new Dictionary<int, int>());
            var secondMiddleware = new StubMessageHandlingMiddleware(2, middlewareOrderingMap, new Dictionary<int, int>());
            var thirdMiddleware = new StubMessageHandlingMiddleware(3, middlewareOrderingMap, new Dictionary<int, int>());
            var middlewares = new List<IMessageHandlingMiddleware>
            {
                firstMiddleware,
                secondMiddleware,
                thirdMiddleware
            };

            var service = CreateService(
                messageHandlingServiceMock.Object,
                errorProcessingServiceMock.Object,
                middlewares);

            var context = new MessageHandlingContext(eventArgs, AckAction, false);
            await service.Execute(context);

            messageHandlingServiceMock.Verify(x => x.HandleMessageReceivingEvent(It.IsAny<MessageHandlingContext>()), Times.Once);
            Assert.Equal(1, middlewareOrderingMap[thirdMiddleware.Number]);
            Assert.Equal(2, middlewareOrderingMap[secondMiddleware.Number]);
            Assert.Equal(3, middlewareOrderingMap[firstMiddleware.Number]);
        }

        [Fact]
        public async Task ShouldProperlyExecuteFailurePipelineWhenMessageHandlingServiceThrowsException()
        {
            var propertiesMock = new Mock<IReadOnlyBasicProperties>();
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routingKey", propertiesMock.Object, new byte[0]);
            var exception = new Exception();
            var messageHandlingServiceMock = new Mock<IMessageHandlingService>();
            messageHandlingServiceMock.Setup(x => x.HandleMessageReceivingEvent(It.IsAny<MessageHandlingContext>()))
                .ThrowsAsync(exception);
            var errorProcessingServiceMock = new Mock<IErrorProcessingService>();

            var middlewareOrderingMap = new Dictionary<int, int>();
            var firstMiddleware = new StubMessageHandlingMiddleware(1, new Dictionary<int, int>(), middlewareOrderingMap);
            var secondMiddleware = new StubMessageHandlingMiddleware(2, new Dictionary<int, int>(), middlewareOrderingMap);
            var thirdMiddleware = new StubMessageHandlingMiddleware(3, new Dictionary<int, int>(), middlewareOrderingMap);
            var middlewares = new List<IMessageHandlingMiddleware>
            {
                firstMiddleware,
                secondMiddleware,
                thirdMiddleware
            };

            var service = CreateService(
                messageHandlingServiceMock.Object,
                errorProcessingServiceMock.Object,
                middlewares);

            var context = new MessageHandlingContext(eventArgs, AckAction, false);
            await service.Execute(context);

            errorProcessingServiceMock.Verify(x => x.HandleMessageProcessingFailure(It.IsAny<MessageHandlingContext>(), exception), Times.Once);
            Assert.Equal(1, middlewareOrderingMap[thirdMiddleware.Number]);
            Assert.Equal(2, middlewareOrderingMap[secondMiddleware.Number]);
            Assert.Equal(3, middlewareOrderingMap[firstMiddleware.Number]);
        }

        [Fact]
        public async Task ShouldProperlyExecuteForHandlerWithNoMiddlewares()
        {
            var propertiesMock = new Mock<IReadOnlyBasicProperties>();
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routing.key", propertiesMock.Object, new byte[0]);
            var handlerMock = new Mock<IMessageHandler>();
            var errorProcessingServiceMock = new Mock<IErrorProcessingService>();

            var service = CreateService(
                Mock.Of<IMessageHandlingService>(),
                errorProcessingServiceMock.Object,
                Enumerable.Empty<IMessageHandlingMiddleware>());

            var context = new MessageHandlingContext(eventArgs, AckAction, false);
            await service.ExecuteForHandler(context, handlerMock.Object, "routing.key");

            handlerMock.Verify(x => x.Handle(context, "routing.key"), Times.Once);
            errorProcessingServiceMock.Verify(x => x.HandleMessageProcessingFailure(It.IsAny<MessageHandlingContext>(), It.IsAny<Exception>()), Times.Never);
        }

        [Fact]
        public async Task ShouldProperlyExecuteForHandlerAsyncWithNoMiddlewares()
        {
            var propertiesMock = new Mock<IReadOnlyBasicProperties>();
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routing.key", propertiesMock.Object, new byte[0]);
            var asyncHandlerMock = new Mock<IAsyncMessageHandler>();
            var errorProcessingServiceMock = new Mock<IErrorProcessingService>();

            var service = CreateService(
                Mock.Of<IMessageHandlingService>(),
                errorProcessingServiceMock.Object,
                Enumerable.Empty<IMessageHandlingMiddleware>());

            var context = new MessageHandlingContext(eventArgs, AckAction, false);
            await service.ExecuteForHandler(context, asyncHandlerMock.Object, "routing.key");

            asyncHandlerMock.Verify(x => x.Handle(context, "routing.key"), Times.Once);
            errorProcessingServiceMock.Verify(x => x.HandleMessageProcessingFailure(It.IsAny<MessageHandlingContext>(), It.IsAny<Exception>()), Times.Never);
        }

        [Fact]
        public async Task ShouldProperlyExecuteForHandlerWithMiddlewares()
        {
            var propertiesMock = new Mock<IReadOnlyBasicProperties>();
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routing.key", propertiesMock.Object, new byte[0]);
            var handlerMock = new Mock<IMessageHandler>();
            var errorProcessingServiceMock = new Mock<IErrorProcessingService>();

            var middlewareOrderingMap = new Dictionary<int, int>();
            var firstMiddleware = new StubMessageHandlingMiddleware(1, middlewareOrderingMap, new Dictionary<int, int>());
            var secondMiddleware = new StubMessageHandlingMiddleware(2, middlewareOrderingMap, new Dictionary<int, int>());
            var thirdMiddleware = new StubMessageHandlingMiddleware(3, middlewareOrderingMap, new Dictionary<int, int>());
            var middlewares = new List<IMessageHandlingMiddleware>
            {
                firstMiddleware,
                secondMiddleware,
                thirdMiddleware
            };

            var service = CreateService(
                Mock.Of<IMessageHandlingService>(),
                errorProcessingServiceMock.Object,
                middlewares);

            var context = new MessageHandlingContext(eventArgs, AckAction, false);
            await service.ExecuteForHandler(context, handlerMock.Object, "routing.key");

            handlerMock.Verify(x => x.Handle(context, "routing.key"), Times.Once);
            Assert.Equal(1, middlewareOrderingMap[thirdMiddleware.Number]);
            Assert.Equal(2, middlewareOrderingMap[secondMiddleware.Number]);
            Assert.Equal(3, middlewareOrderingMap[firstMiddleware.Number]);
        }

        [Fact]
        public async Task ShouldProperlyExecuteFailurePipelineForHandlerWhenHandlerThrowsException()
        {
            var propertiesMock = new Mock<IReadOnlyBasicProperties>();
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routing.key", propertiesMock.Object, new byte[0]);
            var exception = new Exception("handler error");
            var handlerMock = new Mock<IMessageHandler>();
            handlerMock.Setup(x => x.Handle(It.IsAny<MessageHandlingContext>(), It.IsAny<string>())).Throws(exception);
            var errorProcessingServiceMock = new Mock<IErrorProcessingService>();

            var middlewareOrderingMap = new Dictionary<int, int>();
            var firstMiddleware = new StubMessageHandlingMiddleware(1, new Dictionary<int, int>(), middlewareOrderingMap);
            var secondMiddleware = new StubMessageHandlingMiddleware(2, new Dictionary<int, int>(), middlewareOrderingMap);
            var thirdMiddleware = new StubMessageHandlingMiddleware(3, new Dictionary<int, int>(), middlewareOrderingMap);
            var middlewares = new List<IMessageHandlingMiddleware>
            {
                firstMiddleware,
                secondMiddleware,
                thirdMiddleware
            };

            var service = CreateService(
                Mock.Of<IMessageHandlingService>(),
                errorProcessingServiceMock.Object,
                middlewares);

            var context = new MessageHandlingContext(eventArgs, AckAction, false);
            await service.ExecuteForHandler(context, handlerMock.Object, "routing.key");

            errorProcessingServiceMock.Verify(x => x.HandleMessageProcessingFailure(It.IsAny<MessageHandlingContext>(), exception), Times.Once);
            Assert.Equal(1, middlewareOrderingMap[thirdMiddleware.Number]);
            Assert.Equal(2, middlewareOrderingMap[secondMiddleware.Number]);
            Assert.Equal(3, middlewareOrderingMap[firstMiddleware.Number]);
        }

        [Fact]
        public async Task ShouldProperlyExecuteFailurePipelineForHandlerWhenUnsupportedHandlerType()
        {
            var propertiesMock = new Mock<IReadOnlyBasicProperties>();
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routing.key", propertiesMock.Object, new byte[0]);
            var unsupportedHandler = Mock.Of<IBaseMessageHandler>();
            var errorProcessingServiceMock = new Mock<IErrorProcessingService>();

            var service = CreateService(
                Mock.Of<IMessageHandlingService>(),
                errorProcessingServiceMock.Object,
                Enumerable.Empty<IMessageHandlingMiddleware>());

            var context = new MessageHandlingContext(eventArgs, AckAction, false);
            await service.ExecuteForHandler(context, unsupportedHandler, "routing.key");

            errorProcessingServiceMock.Verify(
                x => x.HandleMessageProcessingFailure(
                    It.IsAny<MessageHandlingContext>(),
                    It.Is<NotSupportedException>(e => e.Message.Contains("is not supported"))),
                Times.Once);
        }

        private static IMessageHandlingPipelineExecutingService CreateService(
            IMessageHandlingService messageHandlingService,
            IErrorProcessingService errorProcessingService,
            IEnumerable<IMessageHandlingMiddleware> middlewares) =>
            new MessageHandlingPipelineExecutingService(messageHandlingService, errorProcessingService, middlewares);

        private static Task AckAction(BasicDeliverEventArgs message) => Task.CompletedTask;
    }
}