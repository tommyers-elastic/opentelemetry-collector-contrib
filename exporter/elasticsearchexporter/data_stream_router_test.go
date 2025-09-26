// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/elasticsearch"
)

type routeTestCase struct {
	name        string
	mode        MappingMode
	scopeName   string
	scopeAttrs  map[string]any
	recordAttrs map[string]any
	want        elasticsearch.Index
}

func createRouteTests(dsType string) []routeTestCase {
	renderWantRoute := func(dsType, dsDataset, dsNamespace string, mode MappingMode) elasticsearch.Index {
		if mode == MappingOTel {
			dsDataset += ".otel"
		}
		return elasticsearch.NewDataStreamIndex(dsType, dsDataset, dsNamespace)
	}

	return []routeTestCase{
		{
			name: "default",
			mode: MappingNone,
			want: renderWantRoute(dsType, defaultDataStreamDataset, defaultDataStreamNamespace, MappingNone),
		},
		{
			name: "otel",
			mode: MappingOTel,
			want: renderWantRoute(dsType, defaultDataStreamDataset, defaultDataStreamNamespace, MappingOTel),
		},
		{
			name:      "default with receiver scope name",
			mode:      MappingNone,
			scopeName: "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper",
			want:      renderWantRoute(dsType, "hostmetricsreceiver", defaultDataStreamNamespace, MappingNone),
		},
		{
			name:      "otel with receiver scope name",
			mode:      MappingOTel,
			scopeName: "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper",
			want:      renderWantRoute(dsType, "hostmetricsreceiver", defaultDataStreamNamespace, MappingOTel),
		},
		{
			name:      "default with non-receiver scope name",
			mode:      MappingNone,
			scopeName: "some_other_scope_name",
			want:      renderWantRoute(dsType, defaultDataStreamDataset, defaultDataStreamNamespace, MappingNone),
		},
		{
			name:      "otel with non-receiver scope name",
			mode:      MappingOTel,
			scopeName: "some_other_scope_name",
			want:      renderWantRoute(dsType, defaultDataStreamDataset, defaultDataStreamNamespace, MappingOTel),
		},
		{
			name:      "otel with elasticsearch.index",
			mode:      MappingOTel,
			scopeName: "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/should/be/ignored",
			recordAttrs: map[string]any{
				"elasticsearch.index": "my-index",
			},
			want: elasticsearch.Index{
				Index: "my-index",
			},
		},
		{
			name:      "otel with data_stream attrs",
			mode:      MappingOTel,
			scopeName: "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/should/be/ignored",
			recordAttrs: map[string]any{
				"data_stream.dataset":   "foo",
				"data_stream.namespace": "bar",
			},
			want: renderWantRoute(dsType, "foo", "bar", MappingOTel),
		},
		{
			name:      "default with non-extensions scope attributes",
			mode:      MappingNone,
			scopeName: "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension",
			scopeAttrs: map[string]any{
				"some_other_attr": "should_be_ignored",
			},
			want: renderWantRoute(dsType, defaultDataStreamDataset, defaultDataStreamNamespace, MappingNone),
		},
		{
			name:      "otel with non-extensions scope attributes",
			mode:      MappingOTel,
			scopeName: "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension",
			scopeAttrs: map[string]any{
				"some_other_attr": "should_be_ignored",
			},
			want: renderWantRoute(dsType, defaultDataStreamDataset, defaultDataStreamNamespace, MappingOTel),
		},
		{
			name:      "default with awsencodingextension scope attributes",
			mode:      MappingNone,
			scopeName: "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension",
			scopeAttrs: map[string]any{
				"encoding.format": "aws.cloudtrail",
			},
			want: renderWantRoute(dsType, "aws.cloudtrail", defaultDataStreamNamespace, MappingNone),
		},
		{
			name:      "otel with awsencodingextension scope attributes",
			mode:      MappingOTel,
			scopeName: "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension",
			scopeAttrs: map[string]any{
				"encoding.format": "aws.cloudtrail",
			},
			want: renderWantRoute(dsType, "aws.cloudtrail", defaultDataStreamNamespace, MappingOTel),
		},
		{
			name:      "awsencodingextension scope attributes that are the wrong type",
			mode:      MappingOTel,
			scopeName: "github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension",
			scopeAttrs: map[string]any{
				"encoding.format": int64(123),
			},
			want: renderWantRoute(dsType, defaultDataStreamDataset, defaultDataStreamNamespace, MappingOTel),
		},
		{
			name:      "awsencodingextension scope attributes and receiver scope name",
			mode:      MappingOTel,
			scopeName: "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper",
			scopeAttrs: map[string]any{
				"encoding.format": "aws.vpcflow",
			},
			// extension-based routing should take precedence over receiver-based routing
			want: renderWantRoute(dsType, "aws.vpcflow", defaultDataStreamNamespace, MappingOTel),
		},
	}
}

func TestRouteLogRecord(t *testing.T) {
	tests := createRouteTests(defaultDataStreamTypeLogs)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router := dynamicDocumentRouter{mode: tc.mode}
			scope := pcommon.NewInstrumentationScope()
			scope.SetName(tc.scopeName)
			for k, v := range tc.scopeAttrs {
				switch val := v.(type) {
				case string:
					scope.Attributes().PutStr(k, val)
				case int64:
					scope.Attributes().PutInt(k, val)
				default:
					t.Fatalf("unsupported attribute type for test %T", v)
				}
			}

			recordAttrMap := pcommon.NewMap()
			fillAttributeMap(recordAttrMap, tc.recordAttrs)

			ds, err := router.routeLogRecord(pcommon.NewResource(), scope, recordAttrMap)
			require.NoError(t, err)
			assert.Equal(t, tc.want, ds)
		})
	}

	t.Run("test data_stream.type for bodymap mode", func(t *testing.T) {
		dsType := "metrics"
		router := dynamicDocumentRouter{mode: MappingBodyMap}
		attrs := pcommon.NewMap()
		attrs.PutStr("data_stream.type", dsType)
		ds, err := router.routeLogRecord(pcommon.NewResource(), pcommon.NewInstrumentationScope(), attrs)
		require.NoError(t, err)
		assert.Equal(t, dsType, ds.Type)
	})
	t.Run("test data_stream.type is not honored for other modes (except bodymap)", func(t *testing.T) {
		dsType := "metrics"
		router := dynamicDocumentRouter{mode: MappingOTel}
		attrs := pcommon.NewMap()
		attrs.PutStr("data_stream.type", dsType)
		ds, err := router.routeLogRecord(pcommon.NewResource(), pcommon.NewInstrumentationScope(), attrs)
		require.NoError(t, err)
		assert.Equal(t, "logs", ds.Type) // should equal to logs
	})

	t.Run("test data_stream.type does not accept values other than logs/metrics", func(t *testing.T) {
		dsType := "random"
		router := dynamicDocumentRouter{mode: MappingBodyMap}
		attrs := pcommon.NewMap()
		attrs.PutStr("data_stream.type", dsType)
		_, err := router.routeLogRecord(pcommon.NewResource(), pcommon.NewInstrumentationScope(), attrs)
		require.Error(t, err, "data_stream.type cannot be other than logs or metrics")
	})
}

func TestRouteDataPoint(t *testing.T) {
	tests := createRouteTests(defaultDataStreamTypeMetrics)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router := dynamicDocumentRouter{mode: tc.mode}
			scope := pcommon.NewInstrumentationScope()
			scope.SetName(tc.scopeName)
			for k, v := range tc.scopeAttrs {
				switch val := v.(type) {
				case string:
					scope.Attributes().PutStr(k, val)
				case int64:
					scope.Attributes().PutInt(k, val)
				default:
					t.Fatalf("unsupported attribute type for test %T", v)
				}
			}

			recordAttrMap := pcommon.NewMap()
			fillAttributeMap(recordAttrMap, tc.recordAttrs)

			ds, err := router.routeDataPoint(pcommon.NewResource(), scope, recordAttrMap)
			require.NoError(t, err)
			assert.Equal(t, tc.want, ds)
		})
	}
}

func TestRouteSpan(t *testing.T) {
	tests := createRouteTests(defaultDataStreamTypeTraces)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router := dynamicDocumentRouter{mode: tc.mode}
			scope := pcommon.NewInstrumentationScope()
			scope.SetName(tc.scopeName)
			for k, v := range tc.scopeAttrs {
				switch val := v.(type) {
				case string:
					scope.Attributes().PutStr(k, val)
				case int64:
					scope.Attributes().PutInt(k, val)
				default:
					t.Fatalf("unsupported attribute type for test %T", v)
				}
			}

			recordAttrMap := pcommon.NewMap()
			fillAttributeMap(recordAttrMap, tc.recordAttrs)

			ds, err := router.routeSpan(pcommon.NewResource(), scope, recordAttrMap)
			require.NoError(t, err)
			assert.Equal(t, tc.want, ds)
		})
	}
}
