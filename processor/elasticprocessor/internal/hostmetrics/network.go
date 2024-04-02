package hostmetrics

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func addNetworkMetrics(metrics pmetric.MetricSlice, resource pcommon.Resource, dataset string) error {
	var timestamp pcommon.Timestamp
	var inBytes, outBytes int64
	// iterate all metrics in the current scope and generate the additional Elastic system integration metrics
	for i := 0; i < metrics.Len(); i++ {
		metric1 := metrics.At(i)
		if metric1.Name() == "system.network.io" {
			dataPoints := metric1.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				timestamp = dp.Timestamp()
				value := dp.IntValue()
				if direction, ok := dp.Attributes().Get("direction"); ok {
					switch direction.Str() {
					case "receive":
						inBytes = value
						addMetrics(metrics, resource, dataset, dp,
							metric{
								dataType:  Sum,
								name:      "system.network.in.bytes",
								timestamp: timestamp,
								intValue:  &inBytes,
							},
						)
					case "transmit":
						outBytes = value
						addMetrics(metrics, resource, dataset, dp,
							metric{
								dataType:  Sum,
								name:      "system.network.out.bytes",
								timestamp: timestamp,
								intValue:  &outBytes,
							},
						)
					}
				}
			}
		}
	}
	return nil
}
