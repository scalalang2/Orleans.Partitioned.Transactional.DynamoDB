using System.Diagnostics;
using System.Reflection;
using Orleans;

namespace SampleApp;

internal sealed class SampleOutgoingGrainTraceFilter : IOutgoingGrainCallFilter
{
    public async Task Invoke(IOutgoingGrainCallContext context)
    {
        var interfaceMethod = context.GetType().GetProperty("InterfaceMethod")?.GetValue(context) as MethodInfo;
        var targetGrain = context.GetType().GetProperty("Grain")?.GetValue(context);

        var operationName = interfaceMethod is not null
            ? $"InventoryClient.{interfaceMethod.Name}"
            : "InventoryClient.Call";

        using var activity = SampleTelemetry.ActivitySource.StartActivity(operationName, ActivityKind.Client);
        activity?.SetTag("orleans.method", interfaceMethod?.Name ?? "Unknown");
        activity?.SetTag("orleans.target", targetGrain?.ToString());
        activity?.SetTag("orleans.source", context.SourceContext?.GrainId.ToString());

        using var _ = SampleTelemetry.PushRequestTraceContext(activity);
        await context.Invoke();
    }
}
