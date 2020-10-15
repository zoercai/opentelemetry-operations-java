import io.opencensus.trace.unsafe.CtxUtils;

public class OpenTelemetryAdapter {
  private OpenTelemetryAdapter() {}

  public static void register() {
    CtxUtils.setContextManager(new OpenTelemetryContextManager());
  }
}
