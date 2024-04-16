package hostmetrics

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type deviceMetrics struct {
	inBytes    int64
	outBytes   int64
	inPackets  int64
	outPackets int64
	inDropped  int64
	outDropped int64
	inErrors   int64
	outErrors  int64
}

func addNetworkMetrics(metrics pmetric.MetricSlice, resource pcommon.Resource, dataset string) error {
	var timestamp pcommon.Timestamp
	metricsByDevice := make(map[string]*deviceMetrics)

	for i := 0; i < metrics.Len(); i++ {
		metric := metrics.At(i)
		if metric.Name() == "system.network.io" {
			dataPoints := metric.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				if timestamp == 0 {
					timestamp = dp.Timestamp()
				}
				value := dp.IntValue()

				var device string
				if d, ok := dp.Attributes().Get("device"); ok {
					device = d.Str()
				} else {
					continue
				}

				if _, ok := metricsByDevice[device]; !ok {
					metricsByDevice[device] = &deviceMetrics{}
				}

				if direction, ok := dp.Attributes().Get("direction"); ok {
					switch direction.Str() {
					case "receive":
						metricsByDevice[device].inBytes = value
					case "transmit":
						metricsByDevice[device].outBytes = value
					}
				}
			}
		} else if metric.Name() == "system.network.packets" {
			dataPoints := metric.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				if timestamp == 0 {
					timestamp = dp.Timestamp()
				}
				value := dp.IntValue()

				var device string
				if d, ok := dp.Attributes().Get("device"); ok {
					device = d.Str()
				} else {
					continue
				}

				if _, ok := metricsByDevice[device]; !ok {
					metricsByDevice[device] = &deviceMetrics{}
				}

				if direction, ok := dp.Attributes().Get("direction"); ok {
					switch direction.Str() {
					case "receive":
						metricsByDevice[device].inPackets = value
					case "transmit":
						metricsByDevice[device].outPackets = value
					}
				}
			}
		} else if metric.Name() == "system.network.dropped" {
			dataPoints := metric.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				if timestamp == 0 {
					timestamp = dp.Timestamp()
				}
				value := dp.IntValue()

				var device string
				if d, ok := dp.Attributes().Get("device"); ok {
					device = d.Str()
				} else {
					continue
				}

				if _, ok := metricsByDevice[device]; !ok {
					metricsByDevice[device] = &deviceMetrics{}
				}

				if direction, ok := dp.Attributes().Get("direction"); ok {
					switch direction.Str() {
					case "receive":
						metricsByDevice[device].inDropped = value
					case "transmit":
						metricsByDevice[device].outDropped = value
					}
				}
			}
		} else if metric.Name() == "system.network.errors" {
			dataPoints := metric.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				if timestamp == 0 {
					timestamp = dp.Timestamp()
				}
				value := dp.IntValue()

				var device string
				if d, ok := dp.Attributes().Get("device"); ok {
					device = d.Str()
				} else {
					continue
				}

				if _, ok := metricsByDevice[device]; !ok {
					metricsByDevice[device] = &deviceMetrics{}
				}

				if direction, ok := dp.Attributes().Get("direction"); ok {
					switch direction.Str() {
					case "receive":
						metricsByDevice[device].inErrors = value
					case "transmit":
						metricsByDevice[device].outErrors = value
					}
				}
			}
		}
	}

	for device, deviceMetrics := range metricsByDevice {
		attributes := pcommon.NewMap()
		attributes.PutStr("system.network.name", device)

		addMetrics(metrics, resource, dataset,
			metric{
				dataType:   Sum,
				name:       "system.network.in.bytes",
				timestamp:  timestamp,
				intValue:   &deviceMetrics.inBytes,
				attributes: &attributes,
			},
			metric{
				dataType:   Sum,
				name:       "system.network.out.bytes",
				timestamp:  timestamp,
				intValue:   &deviceMetrics.outBytes,
				attributes: &attributes,
			},
			metric{
				dataType:   Sum,
				name:       "system.network.in.packets",
				timestamp:  timestamp,
				intValue:   &deviceMetrics.inPackets,
				attributes: &attributes,
			},
			metric{
				dataType:   Sum,
				name:       "system.network.out.packets",
				timestamp:  timestamp,
				intValue:   &deviceMetrics.outPackets,
				attributes: &attributes,
			},
			metric{
				dataType:   Sum,
				name:       "system.network.in.dropped",
				timestamp:  timestamp,
				intValue:   &deviceMetrics.inDropped,
				attributes: &attributes,
			},
			metric{
				dataType:   Sum,
				name:       "system.network.out.dropped",
				timestamp:  timestamp,
				intValue:   &deviceMetrics.outDropped,
				attributes: &attributes,
			},
			metric{
				dataType:   Sum,
				name:       "system.network.in.errors",
				timestamp:  timestamp,
				intValue:   &deviceMetrics.inErrors,
				attributes: &attributes,
			},
			metric{
				dataType:   Sum,
				name:       "system.network.out.errors",
				timestamp:  timestamp,
				intValue:   &deviceMetrics.outErrors,
				attributes: &attributes,
			},
		)
	}

	return nil
}
