import io.opencensus.implcore.trace.RecordEventsSpanImpl;
import io.opencensus.trace.Span;
import java.util.HashMap;
import java.util.Map;

public class SpanCache {
  private static SpanCache spanCache;
  private Map<Span, io.opentelemetry.trace.Span> ocToOt = new HashMap<>();
  private Map<io.opentelemetry.trace.Span, Span> otToOc = new HashMap<>();

  private SpanCache() {}

  public static SpanCache getInstance() {
    if (spanCache == null) {
      spanCache = new SpanCache();
    }
    return spanCache;
  }

  io.opentelemetry.trace.Span toOtelSpan(Span ocSpan) {
    io.opentelemetry.trace.Span otSpan = ocToOt.get(ocSpan);
    if (otSpan == null) {
      otSpan = SpanConverter.toOtelSpan(ocSpan);
      otToOc.put(otSpan, ocSpan);
      ocToOt.put(ocSpan, otSpan);
    }
    return otSpan;
  }

  Span fromOtelSpan(io.opentelemetry.trace.Span otSpan) {
    Span span = otToOc.get(otSpan);
    if (span == null) {
      span = SpanConverter.fromOtelSpan(otSpan);
      ocToOt.put(span, otSpan);
      otToOc.put(otSpan, span);
    }
    return span;
  }

  void removeFromCache(RecordEventsSpanImpl ocSpan) {
    io.opentelemetry.trace.Span otSpan = ocToOt.get(ocSpan);
    ocToOt.remove(ocSpan);
    otToOc.remove(otSpan);
  }
}
