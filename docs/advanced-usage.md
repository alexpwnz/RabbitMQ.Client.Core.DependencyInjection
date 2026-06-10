# Advanced usage

You are allowed to register a RabbitMQ Client as an implementation of two interfaces - `IConsumingService` and `IProducingService`. Each interface defines its own connection and its own collection of methods, obviously, for message production and message consumption. You can also use different credentials for different connections, and there is an option `ClientProvidedName` which allows you to create a "named" connection (which will be easier to find in the RabbitMQ management UI). There is also a possibility of registering `IConsumingService` and `IProducingService` in different lifetime modes, in case you want your consumption connection to be persist (singleton `IConsumingService`) and open a connection each time you want to send a message (a transient `IProducingService`). This situation will be covered in code examples below.

Let' say your application is a web API and you want to use both `IConsumingService` and `IProducingService`. Your `Startup` will look like this.

```c#
public class Startup
{
    IConfiguration Configuration { get; }

    public Startup(IConfiguration configuration)
    {
        Configuration = configuration;
    }

    public void ConfigureServices(IServiceCollection services)
    {
        services.AddControllers();

        // We will use different credentials for connections.
        var rabbitMqConsumerSection = Configuration.GetSection("RabbitMqConsumer");
        var rabbitMqProducerSection = Configuration.GetSection("RabbitMqProducer");

        // And we also configure different exchanges just for a better example.
        var producingExchangeSection = Configuration.GetSection("ProducingExchange");
        var consumingExchangeSection = Configuration.GetSection("ConsumingExchange");

        services.AddRabbitMqConsumingClientSingleton(rabbitMqConsumerSection)
            .AddRabbitMqProducingClientSingleton(rabbitMqProducerSection)
            .AddProductionExchange("exchange.to.send.messages.only", producingExchangeSection)
            .AddConsumptionExchange("consumption.exchange", consumingExchangeSection)
            .AddMessageHandlerTransient<CustomMessageHandler>("routing.key");

        services.AddHostedService<ConsumingHostedService>();
    }

    public void Configure(IApplicationBuilder app)
    {
        app.UseRouting();
        app.UseEndpoints(endpoints => endpoints.MapControllers());
    }
}
```

We have added `IConsumingService` and `IProducingService` via `AddRabbitMqConsumingClientSingleton` and `AddRabbitMqProducingClientSingleton` extension methods. We have also added two exchanges (for different purposes) via `AddProductionExchange` and `AddConsumptionExchange` methods which are covered in previous documentation sections. To start a message consumption we add a custom `IHostedService`, which injects `IConsumingService` and uses its `StartConsumingAsync` method.

```c#
public class ConsumingHostedService : IHostedService
{
    readonly IConsumingService _consumingService;

    public ConsumingHostedService(IConsumingService consumingService)
    {
        _consumingService = consumingService;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await _consumingService.StartConsumingAsync();
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}
```

To send messages we can only use `IProducingService`. Let's inject it inside a controller.

```c#
[ApiController]
[Route("api/example")]
public class ExampleController : ControllerBase
{
    readonly ILogger<ExampleController> _logger;
    readonly IProducingService _producingService;

    public ExampleController(
        IProducingService producingService,
        ILogger<ExampleController> logger)
    {
        _producingService = producingService;
        _logger = logger;
    }

    [HttpGet]
    public async Task<IActionResult> Get()
    {
        _logger.LogInformation($"Sending messages with {typeof(IProducingService)}.");
        var message = new { message = "text" };
        await _producingService.SendAsync(message, "exchange.to.send.messages.only", "some.routing.key");
        return Ok(message);
    }
}
```

And the last thing we have to look at is a configuration file.
```
{
  "RabbitMqConsumer": {
    "ClientProvidedName": "Consumer",
    "TcpEndpoints": [
      {
        "HostName": "127.0.0.1",
        "Port": 5672
      }
    ],
    "Port": "5672",
    "UserName": "user-consumer",
    "Password": "passwordForConsumer"
  },
  "RabbitMqProducer": {
    "ClientProvidedName": "Producer",
    "TcpEndpoints": [
      {
        "HostName": "127.0.0.1",
        "Port": 5672
      }
    ],
    "Port": "5672",
    "UserName": "user-producer",
    "Password": "passwordForProducer"
  },
  "ConsumingExchange": {
    "Queues": [
      {
        "Name": "consuming.queue",
        "RoutingKeys": [ "routing.key" ]
      }
    ]
  },
  "ProducingExchange": {
    "Queues": [
      {
        "Name": "queue.of.producing.exchange",
        "RoutingKeys": [ "produce.messages", "produce.events" ]
      }
    ]
  }
}
```

As you can see, we set up a RabbitMQ client, which will create a connection for message production each time we call a `IProducingService`. We have also configured connections with different names and credentials. The consuming service creates per-handler channels, each with its own prefetch count, queue, and consumer, as described in the [message consumption documentation](message-consumption.md).

For basic message consumption features see the [Previous page](message-consumption.md)