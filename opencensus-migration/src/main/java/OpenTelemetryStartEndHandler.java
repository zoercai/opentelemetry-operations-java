import io.opencensus.implcore.trace.RecordEventsSpanImpl;
import io.opencensus.implcore.trace.RecordEventsSpanImpl.StartEndHandler;
import io.opencensus.trace.Annotation;
import io.opencensus.trace.MessageEvent;
import io.opencensus.trace.export.SpanData;
import io.opencensus.trace.export.SpanData.TimedEvent;
import io.opentelemetry.trace.Span;

public class OpenTelemetryStartEndHandler implements StartEndHandler {

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
      SpanConverter.mapAndAddAnnotation(span, annotation);
    }
    for (TimedEvent<MessageEvent> event : spanData.getMessageEvents().getEvents()) {
      SpanConverter.mapAndAddTimedEvent(span, event);
    }
    span.end();
  }
}
