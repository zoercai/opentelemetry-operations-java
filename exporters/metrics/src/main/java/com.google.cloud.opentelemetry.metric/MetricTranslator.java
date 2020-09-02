package com.google.cloud.opentelemetry.metric;

import static io.opentelemetry.sdk.metrics.data.MetricData.Descriptor.Type.MONOTONIC_DOUBLE;
import static io.opentelemetry.sdk.metrics.data.MetricData.Descriptor.Type.MONOTONIC_LONG;
import static io.opentelemetry.sdk.metrics.data.MetricData.Descriptor.Type.NON_MONOTONIC_DOUBLE;
import static io.opentelemetry.sdk.metrics.data.MetricData.Descriptor.Type.NON_MONOTONIC_LONG;
import static java.util.logging.Level.WARNING;

import com.google.api.LabelDescriptor;
import com.google.api.Metric;
import com.google.api.MetricDescriptor;
import com.google.api.MonitoredResource;
import com.google.cloud.opentelemetry.metric.MetricExporter.MetricWithLabels;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.Point.Builder;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TypedValue;
import com.google.protobuf.Timestamp;
import io.opentelemetry.common.ReadableAttributes;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricData.Descriptor.Type;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class MetricTranslator {

  private static final Logger logger = Logger.getLogger(MetricTranslator.class.getName());

  private static final String DESCRIPTOR_TYPE_URL = "custom.googleapis.com/OpenTelemetry/";
  private static final String UNIQUE_IDENTIFIER_KEY = "opentelemetry_id";
  private static final long NANO_PER_SECOND = (long) 1e9;

  private static final Set<Type> GAUGE_TYPES = ImmutableSet.of(NON_MONOTONIC_LONG, NON_MONOTONIC_DOUBLE);
  private static final Set<MetricData.Descriptor.Type> CUMULATIVE_TYPES = ImmutableSet
      .of(MONOTONIC_LONG, MONOTONIC_DOUBLE);
  private static final Set<MetricData.Descriptor.Type> LONG_TYPES = ImmutableSet.of(NON_MONOTONIC_LONG, MONOTONIC_LONG);
  private static final Set<MetricData.Descriptor.Type> DOUBLE_TYPES = ImmutableSet
      .of(NON_MONOTONIC_DOUBLE, MONOTONIC_DOUBLE);

  private static final Map<String, Map<String, String>> OTEL_TO_GCP_LABELS = ImmutableMap.<String, Map<String, String>>builder()
      .put("gce_instance", ImmutableMap.<String, String>builder()
          .put("host.id", "instance_id")
          .put("cloud.account.id", "project_id")
          .put("cloud.zone", "zone")
          .build())
      .put("gke_container", ImmutableMap.<String, String>builder()
          .put("k8s.cluster.name", "cluster_name")
          .put("k8s.namespace.name", "namespace_id")
          .put("k8s.pod.name", "pod_id")
          .put("host.id", "instance_id")
          .put("container.name", "container_name")
          .put("cloud.account.id", "project_id")
          .put("cloud.zone", "zone")
          .build())
      .build();


  static Metric mapMetric(MetricData metric, MetricDescriptor descriptor, String uniqueIdentifier) {
    Metric.Builder metricBuilder = Metric.newBuilder().setType(descriptor.getType());
    metric.getDescriptor().getConstantLabels().forEach(metricBuilder::putLabels);
    if (uniqueIdentifier != null) {
      metricBuilder.putLabels(UNIQUE_IDENTIFIER_KEY, uniqueIdentifier);
    }
    return metricBuilder.build();
  }

  static MetricDescriptor mapMetricDescriptor(MetricData metric, String uniqueIdentifier) {
    String instrumentName = metric.getInstrumentationLibraryInfo().getName();
    MetricDescriptor.Builder builder = MetricDescriptor.newBuilder().setDisplayName(instrumentName)
        .setType(DESCRIPTOR_TYPE_URL + instrumentName);
    metric.getDescriptor().getConstantLabels().forEach((key, value) -> builder.addLabels(mapConstantLabel(key, value)));
    if (uniqueIdentifier != null) {
      builder.addLabels(
          LabelDescriptor.newBuilder().setKey(UNIQUE_IDENTIFIER_KEY).setValueType(LabelDescriptor.ValueType.STRING)
              .build());
    }

    MetricData.Descriptor.Type metricType = metric.getDescriptor().getType();
    if (GAUGE_TYPES.contains(metricType)) {
      builder.setMetricKind(MetricDescriptor.MetricKind.GAUGE);
    } else if (CUMULATIVE_TYPES.contains(metricType)) {
      builder.setMetricKind(MetricDescriptor.MetricKind.CUMULATIVE);
    } else {
      logger.log(WARNING, "Metric type {} not supported", metricType);
      return null;
    }
    if (LONG_TYPES.contains(metricType)) {
      builder.setValueType(MetricDescriptor.ValueType.INT64);
    } else if (DOUBLE_TYPES.contains(metricType)) {
      builder.setValueType(MetricDescriptor.ValueType.DOUBLE);
    } else {
      logger.log(WARNING, "Metric type {} not supported", metricType);
      return null;
    }
    return builder.build();
  }

  static MonitoredResource mapGcpMonitoredResource(Resource resource) {
    ReadableAttributes attributes = resource.getAttributes();
    if (attributes.get("cloud.provider") != null &&
        !attributes.get("cloud.provider").getStringValue().equals("gcp")) {
      return null;
    }
    String resourceType = attributes.get("gcp.resource_type").getStringValue();
    if (!(resourceType.equalsIgnoreCase("gce_instance") || resourceType.equalsIgnoreCase("gke_container"))) {
      return null;
    }

    MonitoredResource.Builder builder = MonitoredResource.newBuilder().setType(resourceType);
    for (Map.Entry<String, String> labels : OTEL_TO_GCP_LABELS.get(resourceType).entrySet()) {
      if (attributes.get(labels.getKey()) == null) {
        logger.log(WARNING, "Missing monitored resource value for {}", labels.getKey());
        continue;
      }
      builder.putLabels(labels.getValue(), attributes.get(labels.getKey()).getStringValue());
    }
    return builder.build();
  }

  static LabelDescriptor mapConstantLabel(String key, String value) {
    LabelDescriptor.Builder builder = LabelDescriptor.newBuilder().setKey(key);
    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
      builder.setValueType(LabelDescriptor.ValueType.BOOL);
    } else if (Ints.tryParse(value) != null) {
      builder.setValueType(LabelDescriptor.ValueType.INT64);
    } else {
      builder.setValueType(LabelDescriptor.ValueType.STRING);
    }
    return builder.build();
  }

  static Point mapPoint(Map<MetricWithLabels, Long> lastUpdatedTime, MetricData metric, MetricWithLabels updateKey,
      Instant exporterStartTime, long pointCollectionTime) {
    Builder pointBuilder = Point.newBuilder();
    Type type = metric.getDescriptor().getType();
    if (LONG_TYPES.contains(type)) {
      pointBuilder.setValue(TypedValue.newBuilder().setInt64Value(
          ((MetricData.LongPoint) metric.getPoints().iterator().next()).getValue()));
    } else if (DOUBLE_TYPES.contains(type)) {
      pointBuilder.setValue(TypedValue.newBuilder().setDoubleValue(
          ((MetricData.DoublePoint) metric.getPoints().iterator().next()).getValue()));
    } else {
      logger.log(WARNING, "Type {} not supported", type);
      return null;
    }
    pointBuilder.setInterval(
        mapInterval(lastUpdatedTime, updateKey, type, exporterStartTime, pointCollectionTime));
    return pointBuilder.build();
  }

  static TimeInterval mapInterval(Map<MetricWithLabels, Long> lastUpdatedTime,
      MetricWithLabels updateKey, Type descriptorType, Instant exporterStartTime, long pointCollectionTime) {
    long seconds;
    int nanos;
    if (CUMULATIVE_TYPES.contains(descriptorType)) {
      if (!lastUpdatedTime.containsKey(updateKey)) {
        // The aggregation has not reset since the exporter
        // has started up, so that is the start time
        seconds = exporterStartTime.getEpochSecond();
        nanos = exporterStartTime.getNano();
      } else {
        // The aggregation reset the last time it was exported
        // Add 1ms to guarantee there is no overlap from the previous export
        // (see https://cloud.google.com/monitoring/api/ref_v3/rpc/google.monitoring.v3#timeinterval)
        long lastUpdatedNanos = lastUpdatedTime.get(updateKey) + (long) 1e6;
        seconds = lastUpdatedNanos / NANO_PER_SECOND;
        nanos = (int) (lastUpdatedNanos % NANO_PER_SECOND);
      }
    } else {
      seconds = pointCollectionTime / NANO_PER_SECOND;
      nanos = (int) (pointCollectionTime % NANO_PER_SECOND);
    }
    Timestamp startTime = Timestamp.newBuilder().setSeconds(seconds).setNanos(nanos).build();
    Timestamp endTime = Timestamp.newBuilder().setSeconds(pointCollectionTime / NANO_PER_SECOND)
        .setNanos((int) (pointCollectionTime % NANO_PER_SECOND)).build();
    lastUpdatedTime.put(updateKey, pointCollectionTime);
    return TimeInterval.newBuilder().setStartTime(startTime).setEndTime(endTime).build();
  }
}