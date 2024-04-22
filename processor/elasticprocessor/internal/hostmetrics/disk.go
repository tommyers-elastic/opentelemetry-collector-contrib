package hostmetrics

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func addDiskMetrics(metrics pmetric.MetricSlice, resource pcommon.Resource, dataset string) error {
	for i := 0; i < metrics.Len(); i++ {
		metric := metrics.At(i)
		if metric.Name() == "system.disk.io" || metric.Name() == "system.disk.operations" {
			dataPoints := metric.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)

				var device string
				if d, ok := dp.Attributes().Get("device"); ok {
					device = d.Str()
				} else {
					continue
				}

				if direction, ok := dp.Attributes().Get("direction"); ok {
					name := metric.Name()
					timestamp := dp.Timestamp()
					value := dp.IntValue()

					switch direction.Str() {
					case "read":
						addDiskIntMetric(metrics, resource, dataset, name, device, "read", timestamp, value)
					case "write":
						addDiskIntMetric(metrics, resource, dataset, name, device, "write", timestamp, value)
					}
				}
			}
		} else if metric.Name() == "system.disk.operation_time" {
			var multiplier float64
			dataPoints := metric.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)

				var device string
				if d, ok := dp.Attributes().Get("device"); ok {
					device = d.Str()
				} else {
					continue
				}
				if direction, ok := dp.Attributes().Get("direction"); ok {
					name := metric.Name()
					timestamp := dp.Timestamp()
					value := dp.DoubleValue()
					if name == "system.disk.operation_time" {
						multiplier = 1000
					}

					switch direction.Str() {
					case "read":
						addDiskDoubleMetric(metrics, resource, dataset, name, device, "read", timestamp, value, multiplier)
					case "write":
						addDiskDoubleMetric(metrics, resource, dataset, name, device, "write", timestamp, value, multiplier)
					}
				}
			}
		} else if metric.Name() == "system.disk.io_time" {
			var multiplier float64
			dataPoints := metric.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				timestamp := dp.Timestamp()
				value := dp.DoubleValue()
				multiplier = 1000 // Elastic saves this value in milliseconds

				var device string
				if d, ok := dp.Attributes().Get("device"); ok {
					device = d.Str()
				} else {
					continue
				}
				addDiskDoubleMetric(metrics, resource, dataset, metric.Name(), device, "io", timestamp, value, multiplier)
			}
		} else if metric.Name() == "system.disk.pending_operations" {
			var device string
			dataPoints := metric.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				timestamp := dp.Timestamp()
				value := dp.IntValue()

				if d, ok := dp.Attributes().Get("device"); ok {
					device = d.Str()
				} else {
					continue
				}
				addDiskIntMetric(metrics, resource, dataset, metric.Name(), device, "io", timestamp, value)

			}
		}
	}
	return nil
}

// Adds the translated metrics with Int Values
func addDiskIntMetric(metrics pmetric.MetricSlice, resource pcommon.Resource,
	dataset, name, device, esmetricname string, timestamp pcommon.Timestamp, value int64) {

	metricsToAdd := map[string]string{
		"system.disk.io":                 "system.diskio.%s.bytes",
		"system.disk.operations":         "system.diskio.%s.count",
		"system.disk.pending_operations": "system.diskio.%s.ops",
	}

	if metricNetworkES, ok := metricsToAdd[name]; ok {
		attributes := pcommon.NewMap()
		attributes.PutStr("system.diskio.name", device)

		addMetrics(metrics, resource, dataset,
			metric{
				dataType:   Sum,
				name:       fmt.Sprintf(metricNetworkES, esmetricname),
				timestamp:  timestamp,
				intValue:   &value,
				attributes: &attributes,
			})
	}
}

// Adds the translated metrics with Double Values
func addDiskDoubleMetric(metrics pmetric.MetricSlice, resource pcommon.Resource,
	dataset, name, device, esmetricname string, timestamp pcommon.Timestamp, value float64, multiplier float64) {

	metricsToAdd := map[string]string{
		"system.disk.operation_time": "system.diskio.%s.time",
		"system.disk.io_time":        "system.disk.%s.time",
	}

	if metricNetworkES, ok := metricsToAdd[name]; ok {
		attributes := pcommon.NewMap()
		attributes.PutStr("system.diskio.name", device)
		value = value * multiplier

		addMetrics(metrics, resource, dataset,
			metric{
				dataType:    Sum,
				name:        fmt.Sprintf(metricNetworkES, esmetricname),
				timestamp:   timestamp,
				doubleValue: &value,
				attributes:  &attributes,
			})
	}
}
