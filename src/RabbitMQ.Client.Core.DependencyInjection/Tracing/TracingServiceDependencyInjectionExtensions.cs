using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace RabbitMQ.Client.Core.DependencyInjection.Tracing
{
    public static class TracingServiceDependencyInjectionExtensions
    {
        public static IServiceCollection AddRabbitMqTracing(this IServiceCollection services)
        {
            return AddRabbitMqTracing(services, _ => { });
        }

        public static IServiceCollection AddRabbitMqTracing(this IServiceCollection services, Action<ActivityTracingOptions> configure)
        {
            var options = new ActivityTracingOptions();
            configure(options);
            services.AddSingleton(options);
            services.Replace(ServiceDescriptor.Singleton<ITracingService, ActivityTracingService>());
            return services;
        }

        public static IServiceCollection AddCustomMessageTracingService<T>(this IServiceCollection services)
            where T : class, ITracingService
        {
            services.Replace(ServiceDescriptor.Singleton<ITracingService, T>());
            return services;
        }
    }
}
