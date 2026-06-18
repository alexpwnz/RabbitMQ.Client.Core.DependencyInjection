using System;
using System.Text;
using System.Text.Json;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Client.Core.DependencyInjection
{
    public static class BasicDeliverEventArgsExtensions
    {
        public static string GetMessage(this BasicDeliverEventArgs eventArgs)
        {
            eventArgs.EnsureIsNotNull();
            return Encoding.UTF8.GetString(eventArgs.Body.ToArray());
        }

        public static T? GetPayload<T>(this BasicDeliverEventArgs eventArgs)
        {
            eventArgs.EnsureIsNotNull();
            var messageString = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            return JsonSerializer.Deserialize<T>(messageString);
        }

        public static T? GetPayload<T>(this BasicDeliverEventArgs eventArgs, JsonSerializerOptions? options)
        {
            eventArgs.EnsureIsNotNull();
            var messageString = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            return JsonSerializer.Deserialize<T>(messageString, options);
        }

        public static T GetAnonymousPayload<T>(this BasicDeliverEventArgs eventArgs, T anonymousTypeObject)
        {
            eventArgs.EnsureIsNotNull();
            var messageString = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            return JsonSerializer.Deserialize<T>(messageString)!;
        }

        public static T GetAnonymousPayload<T>(this BasicDeliverEventArgs eventArgs, T anonymousTypeObject, JsonSerializerOptions? options)
        {
            eventArgs.EnsureIsNotNull();
            var messageString = Encoding.UTF8.GetString(eventArgs.Body.ToArray());
            return JsonSerializer.Deserialize<T>(messageString, options)!;
        }

        private static BasicDeliverEventArgs EnsureIsNotNull(this BasicDeliverEventArgs eventArgs)
        {
            if (eventArgs is null)
            {
                throw new ArgumentNullException(nameof(eventArgs), "BasicDeliverEventArgs have to be not null to parse a message");
            }

            return eventArgs;
        }
    }
}
