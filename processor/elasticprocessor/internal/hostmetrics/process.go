package hostmetrics

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func addProcessMetrics(metrics pmetric.MetricSlice, dataset string) error {
	var timestamp pcommon.Timestamp
	var threads, memUsage, memVirtual, fdOpen, ioReadBytes, ioWriteBytes, ioReadOperations, ioWriteOperations int64
	var memUtil, memUtilPct, total, cpuTimeValue, systemCpuTime, userCpuTime float64

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
						systemCpuTime = value
						total += value
					case "user":
						userCpuTime = value
						total += value
					case "wait":
						total += value

					}
				}
			}
		} else if metric.Name() == "process.disk.io" {
			dataPoints := metric.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				timestamp = dp.Timestamp()
				value := dp.IntValue()
				if direction, ok := dp.Attributes().Get("direction"); ok {
					switch direction.Str() {
					case "read":
						ioReadBytes = value
					case "write":
						ioWriteBytes = value
					}
				}
			}
		} else if metric.Name() == "process.disk.operations" {
			dataPoints := metric.Sum().DataPoints()
			for j := 0; j < dataPoints.Len(); j++ {
				dp := dataPoints.At(j)
				timestamp = dp.Timestamp()
				value := dp.IntValue()
				if direction, ok := dp.Attributes().Get("direction"); ok {
					switch direction.Str() {
					case "read":
						ioReadOperations = value
					case "write":
						ioWriteOperations = value
					}
				}
			}
		}
	}

	memUtilPct = memUtil / 100
	cpuTimeValue = total * 1000
	systemCpuTime = systemCpuTime * 1000
	userCpuTime = userCpuTime * 1000

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
		metric{
			dataType:    Sum,
			name:        "system.process.cpu.system.ticks",
			timestamp:   timestamp,
			doubleValue: &systemCpuTime,
		},
		metric{
			dataType:    Sum,
			name:        "system.process.cpu.user.ticks",
			timestamp:   timestamp,
			doubleValue: &userCpuTime,
		},
		metric{
			dataType:    Sum,
			name:        "system.process.cpu.total.ticks",
			timestamp:   timestamp,
			doubleValue: &cpuTimeValue,
		},
		metric{
			dataType:  Sum,
			name:      "system.process.io.read_bytes",
			timestamp: timestamp,
			intValue:  &ioReadBytes,
		},
		metric{
			dataType:  Sum,
			name:      "system.process.io.write_bytes",
			timestamp: timestamp,
			intValue:  &ioWriteBytes,
		},
		metric{
			dataType:  Sum,
			name:      "system.process.io.read_ops",
			timestamp: timestamp,
			intValue:  &ioReadOperations,
		},
		metric{
			dataType:  Sum,
			name:      "system.process.io.write_ops",
			timestamp: timestamp,
			intValue:  &ioWriteOperations,
		},
	)

	return nil
}
