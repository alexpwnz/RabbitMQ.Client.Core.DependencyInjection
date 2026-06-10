using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace RabbitMQ.Client.Core.DependencyInjection.Tracing
{
    internal static class TraceContextPropagation
    {
        private const string TraceParentKey = "traceparent";
        private const string TraceStateKey = "tracestate";

        public static void Inject(IDictionary<string, object?> headers)
        {
            var current = Activity.Current;
            if (current?.Id is null)
            {
                return;
            }

            headers[TraceParentKey] = current.Id;
            if (current.TraceStateString is { } traceState)
            {
                headers[TraceStateKey] = traceState;
            }
        }

        public static ActivityContext Extract(IDictionary<string, object?>? headers)
        {
            if (headers is null)
            {
                return default;
            }

            if (!TryGetStringValue(headers, TraceParentKey, out var traceParent) || string.IsNullOrEmpty(traceParent))
            {
                return default;
            }

            TryGetStringValue(headers, TraceStateKey, out var traceState);

            if (ActivityContext.TryParse(traceParent, traceState, out var context))
            {
                return context;
            }

            return default;
        }

        private static bool TryGetStringValue(IDictionary<string, object?> headers, string key, out string? value)
        {
            value = null;
            if (!headers.TryGetValue(key, out var obj) || obj is null)
            {
                return false;
            }

            switch (obj)
            {
                case string s:
                    value = s;
                    return true;
                case byte[] bytes:
                    value = Encoding.UTF8.GetString(bytes);
                    return true;
                default:
                    return false;
            }
        }
    }
}
