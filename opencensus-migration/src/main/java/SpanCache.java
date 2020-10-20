import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.opencensus.implcore.trace.RecordEventsSpanImpl;
import io.opencensus.trace.Span;
import java.util.concurrent.TimeUnit;

public class SpanCache {

  private static final SpanCache SPAN_CACHE = new SpanCache();

  private static final int MAXIMUM_CACHE_SIZE = 10000;
  private static final int CACHE_EXPIRE_TIME = 10;
  private static final TimeUnit CACHE_EXPIRE_UNIT = TimeUnit.MINUTES;

  private static final CacheLoader<io.opentelemetry.trace.Span, Span> OT_TO_OC_CONVERTER =
      new CacheLoader<io.opentelemetry.trace.Span, Span>() {
        @Override
        public Span load(io.opentelemetry.trace.Span span) {
          return SpanConverter.fromOtelSpan(span);
        }
      };

  private static final LoadingCache<io.opentelemetry.trace.Span, Span> OT_TO_OC =
      CacheBuilder.newBuilder()
          .maximumSize(MAXIMUM_CACHE_SIZE)
          .expireAfterAccess(CACHE_EXPIRE_TIME, CACHE_EXPIRE_UNIT)
          .build(OT_TO_OC_CONVERTER);

  private static final CacheLoader<Span, io.opentelemetry.trace.Span> OC_TO_OT_CONVERTER =
      new CacheLoader<Span, io.opentelemetry.trace.Span>() {
        @Override
        public io.opentelemetry.trace.Span load(Span span) {
          return SpanConverter.toOtelSpan(span);
        }
      };

  private static final LoadingCache<Span, io.opentelemetry.trace.Span> OC_TO_OT =
      CacheBuilder.newBuilder()
          .maximumSize(MAXIMUM_CACHE_SIZE)
          .expireAfterAccess(CACHE_EXPIRE_TIME, CACHE_EXPIRE_UNIT)
          .build(OC_TO_OT_CONVERTER);


  public static SpanCache getInstance() {
    return SPAN_CACHE;
  }

  private SpanCache() {}

  io.opentelemetry.trace.Span toOtelSpan(Span ocSpan) {
    io.opentelemetry.trace.Span otSpan = OC_TO_OT.getUnchecked(ocSpan);
    OT_TO_OC.put(otSpan, ocSpan);
    return otSpan;
  }

  Span fromOtelSpan(io.opentelemetry.trace.Span otSpan) {
    Span span = OT_TO_OC.getUnchecked(otSpan);
    OC_TO_OT.put(span, otSpan);
    return span;
  }

  void removeFromCache(RecordEventsSpanImpl ocSpan) {
    io.opentelemetry.trace.Span otSpan = OC_TO_OT.getIfPresent(ocSpan);
    if (otSpan != null) {
      OC_TO_OT.invalidate(ocSpan);
      OT_TO_OC.invalidate(otSpan);
    }
  }
}
