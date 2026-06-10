using System;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Models
{
    public class MessageHandlingContext
    {
        private readonly Func<BasicDeliverEventArgs, Task> _ackAction;
        private bool _alreadyAcknowledged;

        public MessageHandlingContext(BasicDeliverEventArgs message, Func<BasicDeliverEventArgs, Task> ackAction, bool disableAutoAck)
        {
            Message = message;
            _ackAction = ackAction;
            AutoAckEnabled = !disableAutoAck;
        }

        public BasicDeliverEventArgs Message { get; }

        public bool AutoAckEnabled { get; }

        public async Task AcknowledgeMessage()
        {
            if (_alreadyAcknowledged)
            {
                return;
            }

            await _ackAction(Message).ConfigureAwait(false);
            _alreadyAcknowledged = true;
        }
    }
}
