import io.opencensus.common.Clock;
import io.opencensus.impl.trace.internal.ThreadLocalRandomHandler;
import io.opencensus.implcore.common.MillisClock;
import io.opencensus.implcore.trace.RecordEventsSpanImpl.StartEndHandler;
import io.opencensus.implcore.trace.TracerImpl;
import io.opencensus.implcore.trace.config.TraceConfigImpl;
import io.opencensus.implcore.trace.internal.RandomHandler;
import io.opencensus.implcore.trace.propagation.PropagationComponentImpl;
import io.opencensus.trace.TraceComponent;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.export.ExportComponent;
import io.opencensus.trace.propagation.PropagationComponent;

/** Java 8 implementation of the {@link TraceComponent} for OpenTelemetry migration. */
public final class OpenTelemetryTraceComponentImpl extends TraceComponent {
  private final PropagationComponent propagationComponent = new PropagationComponentImpl();
  private final ExportComponent noopExportComponent = ExportComponent.newNoopExportComponent();
  private final Clock clock;
  private final TraceConfig traceConfig = new TraceConfigImpl();
  private final Tracer tracer;

  /** Public constructor to be used with reflection loading. */
  public OpenTelemetryTraceComponentImpl() {
    clock = MillisClock.getInstance();
    RandomHandler randomHandler = new ThreadLocalRandomHandler();
    StartEndHandler startEndHandler = new OpenTelemetryStartEndHandler();
    tracer = new TracerImpl(randomHandler, startEndHandler, clock, traceConfig);
  }

  public Tracer getTracer() {
    return tracer;
  }

  public PropagationComponent getPropagationComponent() {
    return propagationComponent;
  }

  public final Clock getClock() {
    return clock;
  }

  public ExportComponent getExportComponent() {
    // Drop all OC spans, we will export converted OT spans instead
    return noopExportComponent;
  }

  public TraceConfig getTraceConfig() {
    return traceConfig;
  }
}
