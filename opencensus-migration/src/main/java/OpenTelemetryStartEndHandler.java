import io.opencensus.implcore.trace.RecordEventsSpanImpl;
import io.opencensus.implcore.trace.RecordEventsSpanImpl.StartEndHandler;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.MessageEvent;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.export.SpanData.TimedEvent;
import io.opentelemetry.common.AttributeKey;
import io.opentelemetry.common.Attributes;
import io.opentelemetry.trace.Span;

public class OpenTelemetryStartEndHandler implements StartEndHandler {

  private static final long NANOS_PER_SECOND = (long) 1e9;

  private final SpanCache spanCache;

  public OpenTelemetryStartEndHandler() {
    this.spanCache = SpanCache.getInstance();
  }

  @Override
  public void onStart(RecordEventsSpanImpl ocSpan) {
    spanCache.toOtelSpan(ocSpan);
  }

  @Override
  public void onEnd(RecordEventsSpanImpl ocSpan) {
    Span span = spanCache.toOtelSpan(ocSpan);
    SpanData spanData = ocSpan.toSpanData();
    spanCache.removeFromCache(ocSpan);
    for (TimedEvent<Annotation> annotation : spanData.getAnnotations().getEvents()) {
      span.addEvent(
          annotation.getEvent().getDescription(),
          annotation.getTimestamp().getSeconds() * NANOS_PER_SECOND
              + annotation.getTimestamp().getNanos());
    }
    for (TimedEvent<MessageEvent> event : spanData.getMessageEvents().getEvents()) {
      span.addEvent(String.valueOf(event.getEvent().getMessageId()), Attributes.of(
          AttributeKey.stringKey("message.event.type"), event.getEvent().getType().toString(),
          AttributeKey.longKey("message.event.size.uncompressed"), event.getEvent().getUncompressedMessageSize(),
          AttributeKey.longKey("message.event.size.compressed"), event.getEvent().getCompressedMessageSize()
      ), event.getTimestamp().getSeconds() * NANOS_PER_SECOND + event.getTimestamp().getNanos());
    }
    span.end();
  }
}
