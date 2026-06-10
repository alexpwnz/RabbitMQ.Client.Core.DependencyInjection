using System;
using System.Threading.Tasks;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection.Models
{
    public class MessageHandlingContext
    {
        private readonly Func<BasicDeliverEventArgs, Task> _ackAction;
        private readonly Func<BasicDeliverEventArgs, Task> _nackAction;
        private bool _alreadyAcknowledged;
        private bool _alreadyRejected;

        public MessageHandlingContext(BasicDeliverEventArgs message, Func<BasicDeliverEventArgs, Task> ackAction, Func<BasicDeliverEventArgs, Task> nackAction, bool disableAutoAck)
        {
            Message = message;
            _ackAction = ackAction;
            _nackAction = nackAction;
            AutoAckEnabled = !disableAutoAck;
        }

        public BasicDeliverEventArgs Message { get; }

        public bool AutoAckEnabled { get; }

        public bool WasRejected => _alreadyRejected;

        public async Task AcknowledgeMessage()
        {
            if (_alreadyAcknowledged || _alreadyRejected)
            {
                return;
            }

            await _ackAction(Message).ConfigureAwait(false);
            _alreadyAcknowledged = true;
        }

        public async Task RejectMessage()
        {
            if (_alreadyRejected || _alreadyAcknowledged)
            {
                return;
            }

            await _nackAction(Message).ConfigureAwait(false);
            _alreadyRejected = true;
        }
    }
}
