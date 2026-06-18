using System;
using System.Threading.Tasks;
using Moq;
using RabbitMQ.Client.Core.DependencyInjection.Models;
using RabbitMQ.Client.Events;
using Xunit;

namespace RabbitMQ.Client.Core.DependencyInjection.Tests.UnitTests
{
    public class MessageHandlingContextTests
    {
        [Fact]
        public async Task RejectMessageShouldNackWithRequeue()
        {
            var channelMock = new Mock<IChannel>();
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routing.key", null, Array.Empty<byte>());
            Func<BasicDeliverEventArgs, Task> ack = _ => Task.CompletedTask;
            Func<BasicDeliverEventArgs, Task> nack = args => channelMock.Object.BasicNackAsync(args.DeliveryTag, false, true).AsTask();
            var context = new MessageHandlingContext(eventArgs, ack, nack, false);

            await context.RejectMessage();

            channelMock.Verify(x => x.BasicNackAsync(1, false, true, default), Times.Once);
        }

        [Fact]
        public async Task RejectMessageShouldSetWasRejected()
        {
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routing.key", null, Array.Empty<byte>());
            var context = new MessageHandlingContext(eventArgs, _ => Task.CompletedTask, _ => Task.CompletedTask, false);

            Assert.False(context.WasRejected);
            await context.RejectMessage();
            Assert.True(context.WasRejected);
        }

        [Fact]
        public async Task RejectThenAckShouldNotAck()
        {
            var ackCalled = false;
            Func<BasicDeliverEventArgs, Task> ack = _ => { ackCalled = true; return Task.CompletedTask; };
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routing.key", null, Array.Empty<byte>());
            var context = new MessageHandlingContext(eventArgs, ack, _ => Task.CompletedTask, false);

            await context.RejectMessage();
            await context.AcknowledgeMessage();

            Assert.False(ackCalled);
        }

        [Fact]
        public async Task AckThenRejectShouldNotNack()
        {
            var nackCalled = false;
            Func<BasicDeliverEventArgs, Task> nack = _ => { nackCalled = true; return Task.CompletedTask; };
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routing.key", null, Array.Empty<byte>());
            var context = new MessageHandlingContext(eventArgs, _ => Task.CompletedTask, nack, false);

            await context.AcknowledgeMessage();
            await context.RejectMessage();

            Assert.False(nackCalled);
        }

        [Fact]
        public async Task MultipleRejectsShouldOnlyNackOnce()
        {
            var nackCount = 0;
            Func<BasicDeliverEventArgs, Task> nack = _ => { nackCount++; return Task.CompletedTask; };
            var eventArgs = new BasicDeliverEventArgs("tag", 1, false, "exchange", "routing.key", null, Array.Empty<byte>());
            var context = new MessageHandlingContext(eventArgs, _ => Task.CompletedTask, nack, false);

            await context.RejectMessage();
            await context.RejectMessage();
            await context.RejectMessage();

            Assert.Equal(1, nackCount);
        }
    }
}
