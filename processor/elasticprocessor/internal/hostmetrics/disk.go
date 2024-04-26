package hostmetrics

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"golang.org/x/exp/constraints"
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
				var multiplier int64 = 1
				if direction, ok := dp.Attributes().Get("direction"); ok {
					name := metric.Name()
					timestamp := dp.Timestamp()
					value := dp.IntValue()
					//addDiskIntMetric(metrics, resource, dataset, name, device, direction.Str(), timestamp, value)
					addDiskMetric(metrics, resource, dataset, name, device, direction.Str(), timestamp, value, multiplier)
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
					//addDiskDoubleMetric(metrics, resource, dataset, name, device, direction.Str(), timestamp, value, multiplier)
					addDiskMetric(metrics, resource, dataset, name, device, direction.Str(), timestamp, value, multiplier)
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
				//addDiskDoubleMetric(metrics, resource, dataset, ), device, "io", timestamp, value, multiplier)
				addDiskMetric(metrics, resource, dataset, metric.Name(), device, "", timestamp, value, multiplier)
			}
		} else if metric.Name() == "system.disk.pending_operations" {
			dataPoints := metric.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				timestamp := dp.Timestamp()
				value := dp.IntValue()

				var device string
				if d, ok := dp.Attributes().Get("device"); ok {
					device = d.Str()
				} else {
					continue
				}
				var multiplier int64 = 1

				addDiskMetric(metrics, resource, dataset, metric.Name(), device, "", timestamp, value, multiplier)
				//addDiskIntMetric(metrics, resource, dataset, metric.Name(), device, "io", timestamp, value)
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
		"system.disk.io_time":        "system.diskio.%s.time",
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

func addDiskMetric[T interface {
	constraints.Integer | constraints.Float
}](metrics pmetric.MetricSlice, resource pcommon.Resource,
	dataset, name, device, direction string, timestamp pcommon.Timestamp, value, multiplier T) {

	// func addDiskMetric(metrics pmetric.MetricSlice, resource pcommon.Resource,
	// 	dataset, name, device, direction string, timestamp pcommon.Timestamp, value any, multiplier any) {

	metricsToAdd := map[string]string{
		"system.disk.io":                 "system.diskio.%s.bytes",
		"system.disk.operations":         "system.diskio.%s.count",
		"system.disk.pending_operations": "system.diskio.io.%sops",
		"system.disk.operation_time":     "system.diskio.%s.time",
		"system.disk.io_time":            "system.diskio.io.%stime",
	}

	if metricNetworkES, ok := metricsToAdd[name]; ok {
		attributes := pcommon.NewMap()
		attributes.PutStr("system.diskio.name", device)

		var intValue int64
		var doubleValue float64
		scaledValue := value * multiplier
		if i, ok := any(scaledValue).(int64); ok {
			intValue = i
		} else if d, ok := any(scaledValue).(float64); ok {
			doubleValue = d
		}

		addMetrics(metrics, resource, dataset,
			metric{
				dataType:    Sum,
				name:        fmt.Sprintf(metricNetworkES, direction),
				timestamp:   timestamp,
				intValue:    &intValue,
				doubleValue: &doubleValue,
				attributes:  &attributes,
			})
	}
}
