import akka.stream.*;
import akka.stream.stage.*;

import java.util.ArrayList;
import java.util.List;

public class Zipper<I> extends GraphStage<FlowShape<I, List<I>>> {

  private final Inlet<I> in = Inlet.create("in");
  private final Outlet<List<I>> out = Outlet.create("out");
  private final Integer size;

  private final FlowShape<I, List<I>> shape = new FlowShape<>(in, out);

  public Zipper(Integer size) {
    this.size = size > 0 ? size : 1;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
    return new GraphStageLogicWithLoggingAndHandlers<>(shape, in, out, size);
  }

  @Override
  public FlowShape<I, List<I>> shape() {
    return shape;
  }


  public static class GraphStageLogicWithLoggingAndHandlers<I> extends GraphStageLogicWithLogging implements InHandler, OutHandler {
    private final Inlet<I> in;
    private final Outlet<List<I>> out;
    private final Integer size;
    private List<I> zippedList = new ArrayList<>();

    public GraphStageLogicWithLoggingAndHandlers(Shape _shape, Inlet<I> in, Outlet<List<I>> out, Integer size) {
      super(_shape);
      this.in = in;
      this.out = out;
      this.size = size;
      setHandler(in, this);
      setHandler(out, this);
    }


    @Override
    public void onUpstreamFinish() throws Exception {
      if (!zippedList.isEmpty()) {
        emit(out, new ArrayList<>(zippedList));
        zippedList = new ArrayList<>();
      }
    }

    @Override
    public void onPush() throws Exception {
      I msg = grab(in);
      zippedList.add(msg);
      if (zippedList.size() >= size) {
        emit(out, new ArrayList<>(zippedList));
        zippedList = new ArrayList<>();
      }
      pull(in);
    }

    @Override
    public void onPull() throws Exception {
      if (!isClosed(in) && !hasBeenPulled(in)) {
        pull(in);
      }
    }
  }
}
