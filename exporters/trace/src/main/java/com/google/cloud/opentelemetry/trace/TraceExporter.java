package com.google.cloud.opentelemetry.trace;

import static com.google.api.client.util.Preconditions.checkNotNull;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.trace.v2.TraceServiceClient;
import com.google.cloud.trace.v2.TraceServiceSettings;
import com.google.cloud.trace.v2.stub.TraceServiceStub;
import com.google.devtools.cloudtrace.v2.AttributeValue;
import com.google.devtools.cloudtrace.v2.ProjectName;
import com.google.devtools.cloudtrace.v2.Span;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.TracerSdkProvider;
import io.opentelemetry.sdk.trace.config.TraceConfig;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TraceExporter implements SpanExporter {

  private static final Tracer EXPORTER_TRACER =
      OpenTelemetry.builder()
          .setTracerProvider(
              TracerSdkProvider.builder()
                  .setTraceConfig(
                      TraceConfig.getDefault().toBuilder().setSampler(Sampler.alwaysOff()).build())
                  .build())
          .build()
          .getTracer("CloudTraceExporter");

  private final CloudTraceClient cloudTraceClient;
  private final ProjectName projectName;
  private final String projectId;
  private final Map<String, AttributeValue> fixedAttributes;

  public static TraceExporter createWithDefaultConfiguration() throws IOException {
    TraceConfiguration configuration = TraceConfiguration.builder().build();
    return TraceExporter.createWithConfiguration(configuration);
  }

  public static TraceExporter createWithConfiguration(TraceConfiguration configuration)
      throws IOException {
    String projectId = configuration.getProjectId();
    TraceServiceStub stub = configuration.getTraceServiceStub();

    if (stub == null) {
      Credentials credentials =
          configuration.getCredentials() == null
              ? GoogleCredentials.getApplicationDefault()
              : configuration.getCredentials();

      return TraceExporter.createWithCredentials(
          projectId, credentials, configuration.getFixedAttributes(), configuration.getDeadline());
    }
    return TraceExporter.createWithClient(
        projectId,
        new CloudTraceClientImpl(TraceServiceClient.create(stub)),
        configuration.getFixedAttributes());
  }

  private static TraceExporter createWithClient(
      String projectId,
      CloudTraceClient cloudTraceClient,
      Map<String, AttributeValue> fixedAttributes) {
    return new TraceExporter(projectId, cloudTraceClient, fixedAttributes);
  }

  private static TraceExporter createWithCredentials(
      String projectId,
      Credentials credentials,
      Map<String, AttributeValue> fixedAttributes,
      Duration deadline)
      throws IOException {
    TraceServiceSettings.Builder builder =
        TraceServiceSettings.newBuilder()
            .setCredentialsProvider(
                FixedCredentialsProvider.create(checkNotNull(credentials, "credentials")));
    // We only use the batchWriteSpans API in this exporter.
    builder
        .batchWriteSpansSettings()
        .setSimpleTimeoutNoRetries(org.threeten.bp.Duration.ofMillis(deadline.toMillis()));
    return new TraceExporter(
        projectId,
        new CloudTraceClientImpl(TraceServiceClient.create(builder.build())),
        fixedAttributes);
  }

  TraceExporter(
      String projectId,
      CloudTraceClient cloudTraceClient,
      Map<String, AttributeValue> fixedAttributes) {
    this.projectId = projectId;
    this.cloudTraceClient = cloudTraceClient;
    this.projectName = ProjectName.of(projectId);
    this.fixedAttributes = fixedAttributes;
  }

  // TODO @imnoahcook add support for flush
  @Override
  public CompletableResultCode flush() {
    return CompletableResultCode.ofFailure();
  }

  @Override
  public CompletableResultCode export(Collection<SpanData> spanDataList) {
    io.opentelemetry.api.trace.Span notSampledSpan =
        EXPORTER_TRACER
            .spanBuilder(
                "Span that is not sampled, "
                    + "for preventing infinite loop created by cloud trace libraries creating more spans.")
            .startSpan();
    try (Scope scope = notSampledSpan.makeCurrent()) {
      List<Span> spans = new ArrayList<>(spanDataList.size());
      for (SpanData spanData : spanDataList) {
        spans.add(TraceTranslator.generateSpan(spanData, projectId, fixedAttributes));
      }
      cloudTraceClient.batchWriteSpans(projectName, spans);
    } finally {
      notSampledSpan.end();
    }
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public CompletableResultCode shutdown() {
    this.cloudTraceClient.shutdown();
    return CompletableResultCode.ofSuccess();
  }
}
