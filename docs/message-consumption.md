# Message consumption

### Starting a consumer

The first step that has to be done to retrieve messages from queues is to start a consumer. This can be achieved by calling the `StartConsumingAsync` method of `IConsumingService`.
Consumption exchanges will work only in a message-production mode if `StartConsumingAsync` won't be called.

Let's say that your configuration looks like this.

```c#
public class Startup
{
    public static IConfiguration Configuration;

    public Startup(IConfiguration configuration)
    {
        Configuration = configuration;
    }

    public void ConfigureServices(IServiceCollection services)
    {
        var clientConfiguration = Configuration.GetSection("RabbitMq");
        var exchangeConfiguration = Configuration.GetSection("RabbitMqExchange");
        services.AddRabbitMqClient(clientConfiguration)
            .AddExchange("ExchangeName", isConsuming: true, exchangeConfiguration);
    }
}
```

You can register `IHostedService` and inject an instance of `IConsumingService` into it.

```c#
services.AddSingleton<IHostedService, ConsumingService>();
```

Then simply call `StartConsumingAsync` so a consumer can work in the background. There is also an option which allows you to stop consuming messages — method `StopConsumingAsync` which you can use any time you want to pause message consumption for any reason.

```c#
public class ConsumingService : IHostedService
{
    readonly IConsumingService _consumingService;
    readonly ILogger<ConsumingService> _logger;

    public ConsumingService(
        IConsumingService consumingService,
        ILogger<ConsumingService> logger)
    {
        _consumingService = consumingService;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Starting consuming.");
        await _consumingService.StartConsumingAsync();
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping consuming.");
        await _consumingService.StopConsumingAsync();
    }
}
```

Otherwise, you can implement a worker service template from .Net Core 3 like this.

```c#
public class Program
{
    public static void Main(string[] args)
    {
        CreateHostBuilder(args).Build().Run();
    }

    public static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureServices((hostContext, services) =>
            {
                var clientConfiguration = hostContext.Configuration.GetSection("RabbitMq");
                var exchangeConfiguration = hostContext.Configuration.GetSection("RabbitMqExchange");
                services.AddRabbitMqClient(clientConfiguration)
                    .AddExchange("ExchangeName", isConsuming: true, exchangeConfiguration);

                services.AddHostedService<Worker>();
            });
}

public class Worker : BackgroundService
{
    readonly IConsumingService _consumingService;

    public Worker(IConsumingService consumingService)
    {
        _consumingService = consumingService;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await _consumingService.StartConsumingAsync();
    }
}
```

The second step is to define classes that will take responsibility of handling received messages. There are synchronous and asynchronous message handlers.

### Per-handler channels architecture

Each registered message handler receives its own **dedicated RabbitMQ channel, queue, and consumer**. This means:

- Every handler has its own independent channel with its own prefetch count (QoS) setting.
- A dedicated queue is created per handler per consumption exchange. By default, the queue is named `{FullTypeName}_{ExchangeName}_handler`, but you can override it by specifying the `queueName` parameter.
- The handler's routing keys are used to bind this queue to the exchange.
- Messages are dispatched directly to the handler through the middleware pipeline, bypassing runtime routing.

This design ensures that one handler's prefetch behavior or processing delay does not affect other handlers.

#### General handlers (without exchange)

If a handler is registered without specifying an exchange, it will be bound to **all** consumption exchanges — a dedicated queue and channel will be created for each.

### Synchronous message handlers

`IMessageHandler` consists of one method `Handle` that receives a `MessageHandlingContext`. You can deserialize the message with `BasicDeliverEventArgs` extensions (described below). The handler also receives the matching route string.

```c#
public class CustomMessageHandler : IMessageHandler
{
    public void Handle(MessageHandlingContext context, string matchingRoute)
    {
        // Do whatever you want.
        var messageObject = context.Message.GetPayload<YourClass>();
    }
}
```

You can also inject almost any services inside the `IMessageHandler` constructor.

```c#
public class CustomMessageHandler : IMessageHandler
{
    readonly ILogger<CustomMessageHandler> _logger;
    public CustomMessageHandler(ILogger<CustomMessageHandler> logger)
    {
        _logger = logger;
    }

    public void Handle(MessageHandlingContext context, string matchingRoute)
    {
        _logger.LogInformation($"I got a message {context.Message.GetMessage()} by routing key {matchingRoute}");
    }
}
```

#### Per-handler PrefetchCount

Every handler can optionally override the global consumer prefetch count by implementing the `PrefetchCount` property from `IBaseMessageHandler`. When set to a non-null value, it overrides `RabbitMqServiceOptions.PrefetchCount` (default 16) for that handler's dedicated channel.

```c#
public class CustomMessageHandler : IMessageHandler
{
    readonly ILogger<CustomMessageHandler> _logger;

    // This handler will use PrefetchCount = 5 instead of the global default (16).
    public ushort? PrefetchCount => 5;

    public CustomMessageHandler(ILogger<CustomMessageHandler> logger)
    {
        _logger = logger;
    }

    public void Handle(MessageHandlingContext context, string matchingRoute)
    {
        _logger.LogInformation($"Handling message {context.Message.GetMessage()} by routing key {matchingRoute}");
    }
}
```

If `PrefetchCount` returns `null` (the default), the global value from `RabbitMqServiceOptions.PrefetchCount` is used.

#### Manual message acknowledgement and rejection

`MessageHandlingContext` provides two methods for manual message control:

- **`AcknowledgeMessage()`** — acknowledges the message via `BasicAckAsync`. Use this when processing succeeded and the message should be removed from the queue.
- **`RejectMessage()`** — rejects (nacks) the message with `requeue: true` via `BasicNackAsync`. Use this when processing failed and the message should be returned to the queue for re-delivery.

Both methods are idempotent and mutually exclusive — only the first call has effect, subsequent calls are no-ops.

```c#
public class CustomMessageHandler : IMessageHandler
{
    public void Handle(MessageHandlingContext context, string matchingRoute)
    {
        try
        {
            // Process the message...
            context.AcknowledgeMessage();
        }
        catch
        {
            // Return the message to the queue for re-delivery.
            context.RejectMessage();
        }
    }
}
```

When auto-ack is enabled (the default), the message is automatically acknowledged after the pipeline completes — unless `RejectMessage()` was called. If you call `AcknowledgeMessage()` explicitly inside the handler, the auto-ack at the end is a no-op.

### Asynchronous message handlers

If you want to use an async version there is another interface `IAsyncMessageHandler`.

```c#
public class CustomAsyncMessageHandler : IAsyncMessageHandler
{
    public async Task Handle(MessageHandlingContext context, string matchingRoute)
    {
        // Do whatever you want asynchronously!
    }
}
```

You can also set per-handler `PrefetchCount` on async handlers.

```c#
public class CustomAsyncMessageHandler : IAsyncMessageHandler
{
    public ushort? PrefetchCount => 10;

    public async Task Handle(MessageHandlingContext context, string matchingRoute)
    {
        await Task.Delay(100);
        var payload = context.Message.GetPayload<YourClass>();
    }
}
```

### Message handlers registering

The third and final step is to register defined message handlers and let them "listen" for messages relying on specified rules. If there are no message handlers registered then received messages will not be processed.
You can register `IMessageHandler` in your `Startup` calling one of `AddMessageHandler`-ish methods. You are allowed to add message handlers in two modes, **singleton** or **transient**, and there are extension methods for each mode and each message handler type:

- `AddMessageHandlerTransient`
- `AddMessageHandlerSingleton`
- `AddAsyncMessageHandlerTransient`
- `AddAsyncMessageHandlerSingleton`

And this will look like this in your `Startup` code.

```c#
public class Startup
{
    public static IConfiguration Configuration;

    public Startup(IConfiguration configuration)
    {
        Configuration = configuration;
    }

    public void ConfigureServices(IServiceCollection services)
    {
        var clientConfiguration = Configuration.GetSection("RabbitMq");
        var exchangeConfiguration = Configuration.GetSection("RabbitMqExchange");
        services.AddRabbitMqClient(clientConfiguration)
            .AddExchange("ExchangeName", isConsuming: true, exchangeConfiguration)
            .AddMessageHandlerSingleton<CustomMessageHandler>("routing.key");
    }
}
```

RabbitMQ client and exchange configuration sections are not specified in this example, but covered [here](rabbit-configuration.md) and [here](exchange-configuration.md).

Each registered handler gets its own dedicated channel, queue, and consumer as described in the architecture section above.

Message handlers can "listen" for messages by the **specified routing key**, or a **collection of routing keys**. If it is necessary, you can also register multiple message handler at once.

```c#
services.AddRabbitMqClient(clientConfiguration)
    .AddExchange("ExchangeName", isConsuming: true, exchangeConfiguration)
    .AddMessageHandlerSingleton<CustomMessageHandler>("first.routing.key")
    .AddMessageHandlerSingleton<AnotherCustomMessageHandler>(new[] { "second.routing.key", "third.routing.key" });
```

You can also use **pattern matching** in routes where `*` (star) can substitute for exactly one word and `#` (hash) can substitute for zero or more words.

```c#
services.AddRabbitMqClient(clientConfiguration)
    .AddExchange("ExchangeName", isConsuming: true, exchangeConfiguration)
    .AddMessageHandlerSingleton<CustomMessageHandler>("*.routing.*")
    .AddMessageHandlerSingleton<AnotherCustomMessageHandler>(new[] { "#.key", "third.*" });
```

You are also allowed to specify the exact exchange which will be "listened" by a message handler with the given routing key (or a pattern).

```c#
services.AddRabbitMqClient(clientConfiguration)
    .AddExchange("ExchangeName", isConsuming: true, exchangeConfiguration)
    .AddMessageHandlerSingleton<CustomMessageHandler>("*.*.*", "ExchangeName")
    .AddMessageHandlerSingleton<AnotherCustomMessageHandler>("routing.key", "ExchangeName");
```

#### Custom queue name for a handler

By default, each handler gets an auto-generated queue name: `{FullTypeName}_{ExchangeName}_handler`. You can override this by passing the `queueName` parameter. This is useful when you want to use a queue name defined in your `appsettings.json` or when you need a stable, predictable queue name that doesn't change if the handler class is renamed.

```c#
// The queue will be named "myqueue" instead of "{FullTypeName}_{ExchangeName}_handler"
services.AddRabbitMqClient(clientConfiguration)
    .AddExchange("ExchangeName", isConsuming: true, exchangeConfiguration)
    .AddMessageHandlerSingleton<CustomMessageHandler>(
        "routing.key",
        "ExchangeName",
        queueName: "myqueue");
```

The `queueName` parameter is available on all overloads of `AddMessageHandlerTransient`, `AddMessageHandlerSingleton`, `AddAsyncMessageHandlerTransient`, and `AddAsyncMessageHandlerSingleton`. For general handlers (registered without an exchange parameter), the queue name becomes `{queueName}_{ExchangeName}_handler` — the provided name is used as the base instead of the handler's full type name.

You can also set multiple message handlers for managing messages received by one routing key. This case can happen when you want to divide responsibilities between services (e.g. one contains business logic, and the other writes messages in the database).

```c#
services.AddRabbitMqClient(clientConfiguration)
    .AddExchange("ExchangeName", isConsuming: true, exchangeConfiguration)
    .AddMessageHandlerSingleton<CustomMessageHandler>("first.routing.key")
    .AddMessageHandlerSingleton<AnotherCustomMessageHandler>("first.routing.key")
    .AddMessageHandlerSingleton<OneMoreCustomMessageHandler>("first.routing.key");
```

Since you are allowed to register multiple message handlers for one routing key (or one route pattern) you might want to make it run in a special order. You are allowed to do that too.

```c#
services.AddRabbitMqClient(clientConfiguration)
    .AddExchange("ExchangeName", isConsuming: true, exchangeConfiguration)
    .AddMessageHandlerSingleton<CustomMessageHandler>("first.routing.key", order: 1)
    .AddMessageHandlerSingleton<AnotherCustomMessageHandler>("first.routing.key", order: 20)
    .AddMessageHandlerSingleton<OneMoreCustomMessageHandler>("first.routing.key", order: 300);
```

The higher order value — the more important message handler is. So in the previous code snippet the `OneMoreCustomMessageHandler` will process the received message first, `AnotherCustomMessageHandler` will be the second and `CustomMessageHandler` will be the third one.

You can also combine exchange and order configurations together!
```c#
services.AddRabbitMqClient(clientConfiguration)
    .AddExchange("ExchangeName", isConsuming: true, exchangeConfiguration)
    .AddMessageHandlerSingleton<CustomMessageHandler>("first.routing.key", "an.exchange", order: 1)
    .AddMessageHandlerSingleton<AnotherCustomMessageHandler>("first.routing.key", "an.exchange", order: 20)
    .AddMessageHandlerSingleton<OneMoreCustomMessageHandler>("first.routing.key", "an.exchange", order: 300)
    .AddMessageHandlerSingleton<SecondMessageHandler>("second.routing.key", "other.exchange", order: 10)
    .AddMessageHandlerSingleton<ThirdMessageHandler>("second.routing.key", "other.exchange", order: 20);
```

### Workflow of message handling

The message handling process is organized as follows:

- `ChannelDeclarationService` creates a dedicated RabbitMQ channel, queue, and consumer for each registered handler. A global prefetch count (`RabbitMqServiceOptions.PrefetchCount`, default 16) is applied to all consumer channels, unless a handler provides its own `PrefetchCount` override.
- When a message arrives on a handler's queue, the consumer's `ReceivedAsync` event fires.
- `ConsumingService` catches the event, deserializes the event args into a `MessageHandlingContext`, and delegates it to `IMessageHandlingPipelineExecutingService`.
- The pipeline service runs all registered middlewares and then calls the specific handler's `Handle` method.
- After all handlers and middlewares complete, `ConsumingService` acknowledges the message — unless the handler called `RejectMessage()`, in which case the message is nack'ed with requeue and returned to the queue.
- If any exception occurs, the message is acknowledged anyway and the library checks if it has to be re-sent. If the exchange option `RequeueFailedMessages` is `true`, a header `"re-queue-attempts"` is added and the message is sent again with a delay of `RequeueTimeoutMilliseconds` (default 200 ms). The number of attempts is configurable via `RequeueAttempts`.
- A message that has already been re-sent will not be re-sent again (re-send happens only once per attempt cycle).

### Batch message handlers

There is a feature that you can use in case of necessity of handling messages in batches.
First of all you have to create a class that inherits `BaseBatchMessageHandler`.
You have to set up values for `QueueName` and `PrefetchCount` properties. These values are responsible for the queue that will be read by the message handler, and the size of batches of messages. You can also set a `MessageHandlingPeriod` property value and the method `HandleMessage` will be executed repeatedly so messages in unfilled batches could be processed too, but keep in mind that this property is optional.
Be aware that batch message handlers **do not declare queues**, so if it does not exist an exception will be thrown. Either declare manually or using RabbitMqClient configuration features.

```c#
public class CustomBatchMessageHandler : BaseBatchMessageHandler
{
    readonly ILogger<CustomBatchMessageHandler> _logger;

    public CustomBatchMessageHandler(
        IRabbitMqConnectionFactory rabbitMqConnectionFactory,
        IEnumerable<BatchConsumerConnectionOptions> batchConsumerConnectionOptions,
        ILogger<CustomBatchMessageHandler> logger)
        : base(rabbitMqConnectionFactory, batchConsumerConnectionOptions, logger)
    {
        _logger = logger;
    }

    public override ushort PrefetchCount { get; set; } = 50;

    public override string QueueName { get; set; } = "another.queue.name";

    public override TimeSpan? MessageHandlingPeriod { get; set; } = TimeSpan.FromMilliseconds(500);

    public override Task HandleMessages(IEnumerable<BasicDeliverEventArgs> messages, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Handling a batch of messages.");
        foreach (var message in messages)
        {
            _logger.LogInformation(message.GetMessage());
        }
        return Task.CompletedTask;
    }
}
```

After all you have to register that batch message handler via DI.

```c#
services.AddBatchMessageHandler<CustomBatchMessageHandler>(Configuration.GetSection("RabbitMq"));
```

The message handler will create a separate connection and use it for reading messages.
When the message collection is full to the size of `PrefetchCount` it will be passed to the `HandleMessage` method.

### Parsing extensions

There are some simple extensions for `BasicDeliverEventArgs` class that helps to parse messages. You have to use `RabbitMQ.Client.Core.DependencyInjection` namespace to enable those extensions.
There is an example of using those extensions inside a `Handle` method of `IMessageHandler`.

```c#
public class CustomMessageHandler : IMessageHandler
{
    public void Handle(MessageHandlingContext context, string matchingRoute)
    {
        // Access the raw event args via context.Message.
        var eventArgs = context.Message;

        // You can get string message.
        var stringifiedMessage = eventArgs.GetMessage();

        // Or object payload.
        var payload = eventArgs.GetPayload<YourClass>();

        // Or anonymous object by another example object.
        var anonymousObject = new { message = string.Empty, number = 0 };
        var anonymousPayload = eventArgs.GetAnonymousPayload(anonymousObject);
    }
}
```

You can also pass `JsonSerializerSettings` to `GetPayload` or `GetAnonymousPayload` methods as well as collection of `JsonConverter` in case you use custom serialization.

For message production features see the [Previous page](message-production.md)

For more information about advanced usage of the RabbitMQ client see the [Next page](advanced-usage.md)
