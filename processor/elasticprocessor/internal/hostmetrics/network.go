package hostmetrics

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func addNetworkMetrics(metrics pmetric.MetricSlice, resource pcommon.Resource, dataset string) error {
	var timestamp pcommon.Timestamp
	var inBytes, outBytes, inPackets, outPackets, inDropped, outDropped, inErrors, outErrors int64
	// iterate all metrics in the current scope and generate the additional Elastic system integration metrics
	for i := 0; i < metrics.Len(); i++ {
		metric_network := metrics.At(i)
		if metric_network.Name() == "system.network.io" {
			dataPoints := metric_network.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				if timestamp == 0 {
					timestamp = dp.Timestamp()
				}
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
		} else if metric_network.Name() == "system.network.packets" {
			dataPoints := metric_network.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				if timestamp == 0 {
					timestamp = dp.Timestamp()
				}
				value := dp.IntValue()
				if direction, ok := dp.Attributes().Get("direction"); ok {
					switch direction.Str() {
					case "receive":
						inPackets = value
						addMetrics(metrics, resource, dataset, dp,
							metric{
								dataType:  Sum,
								name:      "system.network.in.packets",
								timestamp: timestamp,
								intValue:  &inPackets,
							},
						)
					case "transmit":
						outPackets = value
						addMetrics(metrics, resource, dataset, dp,
							metric{
								dataType:  Sum,
								name:      "system.network.out.packets",
								timestamp: timestamp,
								intValue:  &outPackets,
							},
						)
					}
				}
			}
		} else if metric_network.Name() == "system.network.dropped" {
			dataPoints := metric_network.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				if timestamp == 0 {
					timestamp = dp.Timestamp()
				}
				value := dp.IntValue()
				if direction, ok := dp.Attributes().Get("direction"); ok {
					switch direction.Str() {
					case "receive":
						inDropped = value
						addMetrics(metrics, resource, dataset, dp,
							metric{
								dataType:  Sum,
								name:      "system.network.in.dropped",
								timestamp: timestamp,
								intValue:  &inDropped,
							},
						)
					case "transmit":
						outDropped = value
						addMetrics(metrics, resource, dataset, dp,
							metric{
								dataType:  Sum,
								name:      "system.network.out.dropped",
								timestamp: timestamp,
								intValue:  &outDropped,
							},
						)
					}
				}
			}
		} else if metric_network.Name() == "system.network.errors" {
			dataPoints := metric_network.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				if timestamp == 0 {
					timestamp = dp.Timestamp()
				}
				value := dp.IntValue()
				if direction, ok := dp.Attributes().Get("direction"); ok {
					switch direction.Str() {
					case "receive":
						inErrors = value
						addMetrics(metrics, resource, dataset, dp,
							metric{
								dataType:  Sum,
								name:      "system.network.in.errors",
								timestamp: timestamp,
								intValue:  &inErrors,
							},
						)
					case "transmit":
						outErrors = value
						addMetrics(metrics, resource, dataset, dp,
							metric{
								dataType:  Sum,
								name:      "system.network.out.errors",
								timestamp: timestamp,
								intValue:  &outErrors,
							},
						)
					}
				}
			}
		}
	}
	return nil
}
