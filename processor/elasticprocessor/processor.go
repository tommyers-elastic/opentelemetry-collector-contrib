package elasticprocessor

import (
	"context"
	"strings"

	"github.com/elastic/opentelemetry-lib/remappers/hostmetrics"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// remapper interface defines the Remap method that should be implemented by different remappers
type remapper interface {
	Remap(pmetric.ScopeMetrics, pmetric.MetricSlice, pcommon.Resource)
}

type ElasticProcessor struct {
	cfg       *Config
	logger    *zap.Logger
	storage   map[string]any
	remappers []remapper
}

func newProcessor(set processor.CreateSettings, cfg *Config) *ElasticProcessor {
	remappers := []remapper{
		hostmetrics.NewRemapper(set.Logger, hostmetrics.WithSystemIntegrationDataset(true)),
	}
	return &ElasticProcessor{
		cfg:       cfg,
		logger:    set.Logger,
		storage:   make(map[string]any),
		remappers: remappers,
	}
}

// processMetrics processes the given metrics and applies remappers if configured.
func (p *ElasticProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetric := md.ResourceMetrics().At(i)
		rm := resourceMetric.Resource()

		for j := 0; j < resourceMetric.ScopeMetrics().Len(); j++ {
			scopeMetric := resourceMetric.ScopeMetrics().At(j)
			// Apply remappers if AddSystemMetrics is enabled in the configuration
			if p.cfg.AddSystemMetrics {
				if len(p.remappers) > 0 {
					// Apply remappers only if the scope name has the prefix "otelcol/hostmetricsreceiver"
					if strings.HasPrefix(scopeMetric.Scope().Name(), "otelcol/hostmetricsreceiver") {
						for _, r := range p.remappers {
							r.Remap(scopeMetric, scopeMetric.Metrics(), rm)
						}
					}

				}
			}
		}
	}
	return md, nil
}

func (p *ElasticProcessor) processLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	return ld, nil
}

func (p *ElasticProcessor) processTraces(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	return td, nil
}
