import io.grpc.Context;
import io.opencensus.trace.Ctx;

public class OpenTelemetryCtx implements Ctx {

  private final Context context;

  public OpenTelemetryCtx(Context context) {
    this.context = context;
  }

  Context getContext() {
    return context;
  }

  @Override
  public Ctx attach() {
    return new OpenTelemetryCtx(context.attach());
  }

  @Override
  public void detach(Ctx ctx) {
    OpenTelemetryCtx impl = (OpenTelemetryCtx) ctx;
    context.detach(impl.context);
  }
}
