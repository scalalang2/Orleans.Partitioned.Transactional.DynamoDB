using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime;

namespace SampleApp;

internal static class SampleTelemetry
{
    public const string ActivitySourceName = "SampleApp";
    private const string TraceParentContextKey = "sample.traceparent";
    private const string TraceStateContextKey = "sample.tracestate";

    public static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    public static void ConfigureActivityDefaults()
    {
        Activity.DefaultIdFormat = ActivityIdFormat.W3C;
        Activity.ForceDefaultIdFormat = true;
    }

    public static IServiceCollection AddSampleTracing(this IServiceCollection services, IConfiguration configuration)
    {
        ConfigureActivityDefaults();

        var otlpEndpoint = configuration["OTEL_EXPORTER_OTLP_ENDPOINT"] ?? "http://localhost:4317";

        services.AddOpenTelemetry()
            .ConfigureResource(resource => resource.AddService("SampleApp"))
            .WithTracing(tracing =>
            {
                tracing
                    .SetSampler(new AlwaysOnSampler())
                    .AddSource(ActivitySourceName)
                    .AddHttpClientInstrumentation()
                    .AddAWSInstrumentation()
                    .AddOtlpExporter(options => { options.Endpoint = new Uri(otlpEndpoint); });
            });

        services.AddSingleton<IOutgoingGrainCallFilter, SampleOutgoingGrainTraceFilter>();
        services.AddSingleton<IIncomingGrainCallFilter, SampleIncomingGrainTraceFilter>();

        return services;
    }

    public static IDisposable PushRequestTraceContext(Activity? activity)
    {
        return new RequestTraceContextScope(activity?.Id, activity?.TraceStateString);
    }

    public static Activity? StartServerActivity(string operationName, string grainType, string grainKey)
    {
        var parentContext = TryExtractParentContext();
        var activity = parentContext is { } context
            ? ActivitySource.StartActivity(operationName, ActivityKind.Server, context)
            : ActivitySource.StartActivity(operationName, ActivityKind.Server);

        activity?.SetTag("orleans.grain.type", grainType);
        activity?.SetTag("orleans.grain.key", grainKey);
        return activity;
    }

    private static ActivityContext? TryExtractParentContext()
    {
        var traceParent = RequestContext.Get(TraceParentContextKey) as string;
        if (string.IsNullOrWhiteSpace(traceParent))
        {
            return null;
        }

        var traceState = RequestContext.Get(TraceStateContextKey) as string;
        return ActivityContext.TryParse(traceParent, traceState, out var parentContext) ? parentContext : null;
    }

    private sealed class RequestTraceContextScope : IDisposable
    {
        private readonly object? previousTraceParent;
        private readonly object? previousTraceState;

        public RequestTraceContextScope(string? traceParent, string? traceState)
        {
            previousTraceParent = RequestContext.Get(TraceParentContextKey);
            previousTraceState = RequestContext.Get(TraceStateContextKey);

            RestoreOrRemove(TraceParentContextKey, traceParent);
            RestoreOrRemove(TraceStateContextKey, traceState);
        }

        public void Dispose()
        {
            RestoreOrRemove(TraceParentContextKey, previousTraceParent);
            RestoreOrRemove(TraceStateContextKey, previousTraceState);
        }

        private static void RestoreOrRemove(string key, object? value)
        {
            if (value is null)
            {
                RequestContext.Remove(key);
                return;
            }

            RequestContext.Set(key, value);
        }
    }
}
