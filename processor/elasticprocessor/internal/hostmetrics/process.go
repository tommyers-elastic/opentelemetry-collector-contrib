package hostmetrics

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func addProcessMetrics(metrics pmetric.MetricSlice, dataset string) error {
	var timestamp pcommon.Timestamp
	var threads, memUsage, memVirtual, fdOpen int64
	var memUtil, total, cpuTimeValue float64

	for i := 0; i < metrics.Len(); i++ {
		metric := metrics.At(i)
		if metric.Name() == "process.threads" {
			dp := metric.Sum().DataPoints().At(0)
			timestamp = dp.Timestamp()
			threads = dp.IntValue()
		} else if metric.Name() == "process.memory.utilization" {
			dp := metric.Gauge().DataPoints().At(0)
			timestamp = dp.Timestamp()
			memUtil = dp.DoubleValue()
		} else if metric.Name() == "process.memory.usage" {
			dp := metric.Sum().DataPoints().At(0)
			timestamp = dp.Timestamp()
			memUsage = dp.IntValue()
		} else if metric.Name() == "process.memory.virtual" {
			dp := metric.Sum().DataPoints().At(0)
			timestamp = dp.Timestamp()
			memVirtual = dp.IntValue()
		} else if metric.Name() == "process.open_file_descriptors" {
			dp := metric.Sum().DataPoints().At(0)
			timestamp = dp.Timestamp()
			fdOpen = dp.IntValue()
		} else if metric.Name() == "process.cpu.time" {
			dataPoints := metric.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				timestamp = dp.Timestamp()
				value := dp.DoubleValue()
				if state, ok := dp.Attributes().Get("state"); ok {
					switch state.Str() {
					case "system":
						total += value
					case "user":
						total += value
					case "wait":
						total += value

					}
				}
			}
		}
	}

	memUtilPct := memUtil / 100
	cpuTimeValue = total * 1000

	addMetrics(metrics, dataset,
		metric{
			dataType:  Sum,
			name:      "system.process.num_threads",
			timestamp: timestamp,
			intValue:  &threads,
		},
		metric{
			dataType:    Gauge,
			name:        "system.process.memory.rss.pct",
			timestamp:   timestamp,
			doubleValue: &memUtilPct,
		},
		metric{
			dataType:  Sum,
			name:      "system.process.memory.rss.bytes",
			timestamp: timestamp,
			intValue:  &memUsage,
		},
		metric{
			dataType:  Sum,
			name:      "system.process.memory.size",
			timestamp: timestamp,
			intValue:  &memVirtual,
		},
		metric{
			dataType:  Sum,
			name:      "system.process.fd.open",
			timestamp: timestamp,
			intValue:  &fdOpen,
		},
		metric{
			dataType:    Gauge,
			name:        "process.memory.pct",
			timestamp:   timestamp,
			doubleValue: &memUtilPct,
		},
		metric{
			dataType:    Sum,
			name:        "system.process.cpu.total.value",
			timestamp:   timestamp,
			doubleValue: &cpuTimeValue,
		},
	)

	return nil
}
