using System.Diagnostics.CodeAnalysis;
using RabbitMQ.Client.Core.DependencyInjection.Exceptions;

namespace RabbitMQ.Client.Core.DependencyInjection.InternalExtensions.Validation
{
    internal static class ChannelValidationExtensions
    {
        internal static IChannel EnsureIsNotNull([NotNull]this IChannel? channel)
        {
            if (channel is null)
            {
                throw new ChannelIsNullException();
            }

            return channel;
        }
    }
}