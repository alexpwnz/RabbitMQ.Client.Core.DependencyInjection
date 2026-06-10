namespace RabbitMQ.Client.Core.DependencyInjection.Tracing
{
    public class ActivityTracingOptions
    {
        public string ActivitySourceName { get; set; } = "RabbitMQ.Client.Core.DependencyInjection";

        public bool TracePublishing { get; set; } = true;

        public bool TraceConsuming { get; set; } = true;
    }
}
