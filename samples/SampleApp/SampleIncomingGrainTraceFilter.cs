using Orleans;

namespace SampleApp;

internal sealed class SampleIncomingGrainTraceFilter : IIncomingGrainCallFilter
{
    public async Task Invoke(IIncomingGrainCallContext context)
    {
        var grainType = context.ImplementationMethod.DeclaringType?.Name ?? "Grain";
        var grainKey = context.TargetContext.GrainId.ToString();

        using var activity = SampleTelemetry.StartServerActivity(
            $"{grainType}.{context.ImplementationMethod.Name}",
            grainType,
            grainKey);

        await context.Invoke();
    }
}
