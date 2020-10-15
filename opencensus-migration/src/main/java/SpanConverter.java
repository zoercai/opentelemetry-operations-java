import io.opencensus.common.Function;
import io.opencensus.implcore.trace.RecordEventsSpanImpl;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.EndSpanOptions;
import io.opencensus.trace.Link;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.TraceOptions;
import io.opencensus.trace.Tracestate;
import io.opencensus.trace.export.SpanData;
import io.opentelemetry.OpenTelemetry;
import io.opentelemetry.trace.Span.Builder;
import io.opentelemetry.trace.SpanId;
import io.opentelemetry.trace.TraceFlags;
import io.opentelemetry.trace.TraceId;
import io.opentelemetry.trace.TraceState;
import io.opentelemetry.trace.Tracer;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;

public class SpanConverter {

  public static class FakeSpan extends Span {

    private static final EnumSet<Options> RECORD_EVENTS_SPAN_OPTIONS =
        EnumSet.of(Options.RECORD_EVENTS);

    protected FakeSpan(SpanContext context) {
      super(context, RECORD_EVENTS_SPAN_OPTIONS);
    }

    @Override
    public void addAnnotation(String description, Map<String, AttributeValue> attributes) {
    }

    @Override
    public void addAnnotation(Annotation annotation) {
    }

    @Override
    public void addLink(Link link) {
    }

    @Override
    public void end(EndSpanOptions options) {
    }
  }

  private static final long NANOS_PER_SECOND = (long) 1e9;
  private static Tracer tracer = OpenTelemetry.getTracer("io.opencensus.opentelemetry.migration");

  static io.opentelemetry.trace.Span toOtelSpan(Span span) {
    if (span == null) {
      return null;
    }
    SpanData spanData = ((RecordEventsSpanImpl) span).toSpanData();
    Builder builder =
        tracer
            .spanBuilder(spanData.getName())
            .setStartTimestamp(
                spanData.getStartTimestamp().getSeconds() * NANOS_PER_SECOND
                    + spanData.getStartTimestamp().getNanos());
    if (spanData.getAttributes() != null) {
      for (Entry<String, AttributeValue> attribute : spanData.getAttributes().getAttributeMap().entrySet()) {
        attribute.getValue().match(
            setStringAttribute(builder, attribute),
            setBooleanAttribute(builder, attribute),
            setLongAttribute(builder, attribute),
            setDoubleAttribute(builder, attribute),
            arg -> null);
      }
    }
    if (spanData.getLinks() != null) {
      for (Link link : spanData.getLinks().getLinks()) {
        builder.addLink(
            io.opentelemetry.trace.SpanContext.create(
                TraceId.bytesToHex(link.getTraceId().getBytes()),
                SpanId.bytesToHex(link.getSpanId().getBytes()),
                TraceFlags.getDefault(),
                TraceState.getDefault()));
      }
    }
    return builder.startSpan();
  }

  static Span fromOtelSpan(io.opentelemetry.trace.Span otSpan) {
    if (otSpan == null) {
      return null;
    }
    SpanContext spanContext =
        SpanContext.create(
            io.opencensus.trace.TraceId.fromLowerBase16(otSpan.getContext().getTraceIdAsHexString()),
            io.opencensus.trace.SpanId.fromLowerBase16(otSpan.getContext().getSpanIdAsHexString()),
            TraceOptions.DEFAULT,
            Tracestate.builder().build());
    return new FakeSpan(spanContext);
  }

  private static Function<Double, Void> setDoubleAttribute(Builder builder, Entry<String, AttributeValue> attribute) {
    return arg -> {
      builder.setAttribute(attribute.getKey(), arg);
      return null;
    };
  }

  private static Function<Long, Void> setLongAttribute(Builder builder, Entry<String, AttributeValue> attribute) {
    return arg -> {
      builder.setAttribute(attribute.getKey(), arg);
      return null;
    };
  }

  private static Function<Boolean, Void> setBooleanAttribute(Builder builder, Entry<String, AttributeValue> attribute) {
    return arg -> {
      builder.setAttribute(attribute.getKey(), arg);
      return null;
    };
  }

  private static Function<String, Void> setStringAttribute(Builder builder, Entry<String, AttributeValue> attribute) {
    return arg -> {
      builder.setAttribute(attribute.getKey(), arg);
      return null;
    };
  }
}
