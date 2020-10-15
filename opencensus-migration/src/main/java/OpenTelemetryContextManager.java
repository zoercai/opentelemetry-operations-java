import io.grpc.Context;
import io.opencensus.trace.ContextManager;
import io.opencensus.trace.Ctx;
import io.opencensus.trace.Span;
import io.opentelemetry.trace.TracingContextUtils;

public class OpenTelemetryContextManager implements ContextManager {

  private final SpanCache spanCache;

  public OpenTelemetryContextManager() {
    this.spanCache = SpanCache.getInstance();
  }

  @Override
  public Ctx currentContext() {
    return wrapContext(Context.current());
  }

  @Override
  public Ctx withValue(Ctx ctx, Span span) {
    OpenTelemetryCtx openTelemetryCtx = (OpenTelemetryCtx) ctx;
    return wrapContext(
        TracingContextUtils.withSpan(spanCache.toOtelSpan(span), unwrapContext(openTelemetryCtx)));
  }

  @Override
  public Span getValue(Ctx ctx) {
    return spanCache.fromOtelSpan(TracingContextUtils.getSpan(unwrapContext(ctx)));
  }

  private static Ctx wrapContext(Context context) {
    return new OpenTelemetryCtx(context);
  }

  private static Context unwrapContext(Ctx ctx) {
    return ((OpenTelemetryCtx) ctx).getContext();
  }
}
