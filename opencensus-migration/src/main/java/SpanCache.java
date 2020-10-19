import io.opencensus.implcore.trace.RecordEventsSpanImpl;
import io.opencensus.trace.Span;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SpanCache {
  private static final SpanCache SPAN_CACHE = new SpanCache();
  private Map<Span, io.opentelemetry.trace.Span> ocToOt = new ConcurrentHashMap<>();
  private Map<io.opentelemetry.trace.Span, Span> otToOc = new ConcurrentHashMap<>();

  private SpanCache() {}

  public static SpanCache getInstance() {
    return SPAN_CACHE;
  }

  io.opentelemetry.trace.Span toOtelSpan(Span ocSpan) {
    io.opentelemetry.trace.Span otSpan = ocToOt.get(ocSpan);
    if (otSpan == null) {
      otSpan = SpanConverter.toOtelSpan(ocSpan);
      otToOc.putIfAbsent(otSpan, ocSpan);
      ocToOt.putIfAbsent(ocSpan, otSpan);
    }
    return otSpan;
  }

  Span fromOtelSpan(io.opentelemetry.trace.Span otSpan) {
    Span span = otToOc.get(otSpan);
    if (span == null) {
      span = SpanConverter.fromOtelSpan(otSpan);
      ocToOt.putIfAbsent(span, otSpan);
      otToOc.putIfAbsent(otSpan, span);
    }
    return span;
  }

  void removeFromCache(RecordEventsSpanImpl ocSpan) {
    io.opentelemetry.trace.Span otSpan = ocToOt.get(ocSpan);
    if (otSpan != null) {
      ocToOt.remove(ocSpan);
      otToOc.remove(otSpan);
    }
  }
}
