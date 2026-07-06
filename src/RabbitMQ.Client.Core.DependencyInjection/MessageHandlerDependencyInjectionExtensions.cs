using Microsoft.Extensions.DependencyInjection;
using System.Collections.Generic;
using System.Linq;
using RabbitMQ.Client.Core.DependencyInjection.InternalExtensions;
using RabbitMQ.Client.Core.DependencyInjection.MessageHandlers;

namespace RabbitMQ.Client.Core.DependencyInjection
{
    /// <summary>
    /// DI extensions for message handlers.
    /// </summary>
    public static class MessageHandlerDependencyInjectionExtensions
    {
        /// <summary>
        /// Add a transient message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePattern">Route pattern.</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerTransient<T>(this IServiceCollection services, string routePattern, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceTransient<IMessageHandler, T>(new[] { routePattern }.ToList(), 0, prefetchCount);

        /// <summary>
        /// Add a transient message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePatterns">Route patterns.</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerTransient<T>(this IServiceCollection services, IEnumerable<string> routePatterns, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceTransient<IMessageHandler, T>(routePatterns.ToList(), 0, prefetchCount);

        /// <summary>
        /// Add a transient message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePattern">Route pattern.</param>
        /// <param name="order">Message handler order.</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerTransient<T>(this IServiceCollection services, string routePattern, int order, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceTransient<IMessageHandler, T>(new[] { routePattern }.ToList(), order, prefetchCount);

        /// <summary>
        /// Add a transient message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePatterns">Route patterns.</param>
        /// <param name="order">Message handler order.</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerTransient<T>(this IServiceCollection services, IEnumerable<string> routePatterns, int order, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceTransient<IMessageHandler, T>(routePatterns.ToList(), order, prefetchCount);

        /// <summary>
        /// Add a singleton message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePattern">Route pattern.</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerSingleton<T>(this IServiceCollection services, string routePattern, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceSingleton<IMessageHandler, T>(new[] { routePattern }.ToList(), 0, prefetchCount);

        /// <summary>
        /// Add a singleton message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePatterns">Route patterns.</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerSingleton<T>(this IServiceCollection services, IEnumerable<string> routePatterns, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceSingleton<IMessageHandler, T>(routePatterns.ToList(), 0, prefetchCount);

        /// <summary>
        /// Add a singleton message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePattern">Route pattern.</param>
        /// <param name="order">Message handler order.</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerSingleton<T>(this IServiceCollection services, string routePattern, int order, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceSingleton<IMessageHandler, T>(new[] { routePattern }.ToList(), order, prefetchCount);

        /// <summary>
        /// Add a singleton message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePatterns">Route patterns.</param>
        /// <param name="order">Message handler order.</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerSingleton<T>(this IServiceCollection services, IEnumerable<string> routePatterns, int order, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceSingleton<IMessageHandler, T>(routePatterns.ToList(), order, prefetchCount);

        /// <summary>
        /// Add a transient message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePattern">Route pattern.</param>
        /// <param name="exchange">An exchange which will be "listened".</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerTransient<T>(this IServiceCollection services, string routePattern, string exchange, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceTransient<IMessageHandler, T>(new[] { routePattern }.ToList(), exchange, 0, prefetchCount);

        /// <summary>
        /// Add a transient message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePatterns">Route patterns.</param>
        /// <param name="exchange">An exchange which will be "listened".</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerTransient<T>(this IServiceCollection services, IEnumerable<string> routePatterns, string exchange, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceTransient<IMessageHandler, T>(routePatterns.ToList(), exchange, 0, prefetchCount);

        /// <summary>
        /// Add a transient message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePattern">Route pattern.</param>
        /// <param name="exchange">An exchange which will be "listened".</param>
        /// <param name="order">Message handler order.</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerTransient<T>(this IServiceCollection services, string routePattern, string exchange, int order, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceTransient<IMessageHandler, T>(new[] { routePattern }.ToList(), exchange, order, prefetchCount);

        /// <summary>
        /// Add a transient message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePatterns">Route patterns.</param>
        /// <param name="exchange">An exchange which will be "listened".</param>
        /// <param name="order">Message handler order.</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerTransient<T>(this IServiceCollection services, IEnumerable<string> routePatterns, string exchange, int order, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceTransient<IMessageHandler, T>(routePatterns.ToList(), exchange, order, prefetchCount);

        /// <summary>
        /// Add a singleton message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePattern">Route pattern.</param>
        /// <param name="exchange">An exchange which will be "listened".</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerSingleton<T>(this IServiceCollection services, string routePattern, string exchange, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceSingleton<IMessageHandler, T>(new[] { routePattern }.ToList(), exchange, 0, prefetchCount);

        /// <summary>
        /// Add a singleton message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePatterns">Route patterns.</param>
        /// <param name="exchange">An exchange which will be "listened".</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerSingleton<T>(this IServiceCollection services, IEnumerable<string> routePatterns, string exchange, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceSingleton<IMessageHandler, T>(routePatterns.ToList(), exchange, 0, prefetchCount);

        /// <summary>
        /// Add a singleton message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePattern">Route pattern.</param>
        /// <param name="exchange">An exchange which will be "listened".</param>
        /// <param name="order">Message handler order.</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerSingleton<T>(this IServiceCollection services, string routePattern, string exchange, int order, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceSingleton<IMessageHandler, T>(new[] { routePattern }.ToList(), exchange, order, prefetchCount);

        /// <summary>
        /// Add a singleton message handler.
        /// </summary>
        /// <typeparam name="T">Message handler type.</typeparam>
        /// <param name="services">Service collection.</param>
        /// <param name="routePatterns">Route patterns.</param>
        /// <param name="exchange">An exchange which will be "listened".</param>
        /// <param name="order">Message handler order.</param>
        /// <returns>Service collection.</returns>
        public static IServiceCollection AddMessageHandlerSingleton<T>(this IServiceCollection services, IEnumerable<string> routePatterns, string exchange, int order, ushort? prefetchCount = null)
            where T : class, IMessageHandler =>
            services.AddInstanceSingleton<IMessageHandler, T>(routePatterns.ToList(), exchange, order, prefetchCount);
    }
}