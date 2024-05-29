package elasticprocessor

import (
	"context"
	"strings"

	"github.com/elastic/opentelemetry-lib/remappers/hostmetrics"
	//	"github.com/tommyers-elastic/opentelemetry-collector-contrib/processor/elasticprocessor/internal/hostmetrics"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

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

func (p *ElasticProcessor) processMetrics(_ context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	p.logger.Info("Starting to process metrics", zap.Int("resourceMetricsCount", md.ResourceMetrics().Len()))
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetric := md.ResourceMetrics().At(i)
		rm := resourceMetric.Resource()

		for j := 0; j < resourceMetric.ScopeMetrics().Len(); j++ {
			scopeMetric := resourceMetric.ScopeMetrics().At(j)
			if p.cfg.AddSystemMetrics {
				p.logger.Info("Add System Metrics set to true")
				if len(p.remappers) > 0 {
					if strings.HasPrefix(scopeMetric.Scope().Name(), "otelcol/hostmetricsreceiver") {
						//hostmetrics.AddElasticSystemMetrics(scopeMetric, rm, p.storage)
						p.logger.Debug("Remapping metrics", zap.String("scopeName", scopeMetric.Scope().Name()))
						for _, r := range p.remappers {
							r.Remap(scopeMetric, scopeMetric.Metrics(), rm)
						}
						p.logger.Debug("Finished remapping metrics", zap.String("scopeName", scopeMetric.Scope().Name()))
					}

				}
			}
		}
	}
	p.logger.Info("Finished processing metrics", zap.Int("resourceMetricsCount", md.ResourceMetrics().Len()))
	return md, nil
}

func (p *ElasticProcessor) processLogs(_ context.Context, ld plog.Logs) (plog.Logs, error) {
	return ld, nil
}

func (p *ElasticProcessor) processTraces(_ context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	return td, nil
}
