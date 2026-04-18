using System.Diagnostics;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Prometheus;
using Zamza.Server.Application.Common;

namespace Zamza.Server.ConsumerApi.Interceptors;

internal sealed class MetricsInterceptor : Interceptor
{
    private static readonly string InstanceId = ObservabilityContstants.ServiceInstanceId.ToString();
    private static readonly Histogram RequestDurationHistogram = Metrics.CreateHistogram(
        "zamza_consumer_api_requests_duration_seconds",
        "Response times for Zamza.Consumer API requests",
        new HistogramConfiguration
        {
            LabelNames = ["server_instance", "method"],
            Buckets = Histogram.ExponentialBuckets(0.01, 2, 12)
        });

    public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        UnaryServerMethod<TRequest, TResponse> continuation)
    {
        var stopWatch = Stopwatch.StartNew();
        try
        {
            return await continuation(request, context);
        }
        finally
        {
            stopWatch.Stop();
            RequestDurationHistogram
                .WithLabels(
                    [
                        InstanceId,
                        context.Method
                    ])
                .Observe(stopWatch.Elapsed.TotalSeconds);
        }
    }
}