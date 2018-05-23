import akka.japi.function.Procedure;
import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.Shape;
import akka.stream.SourceShape;
import akka.stream.stage.*;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;

public class BatchSource<I> extends GraphStage<SourceShape<List<I>>> {

  private final Outlet<List<I>> outlet = Outlet.create("out");
  private final BiFunction<Integer, Integer, CompletionStage<List<I>>> objectSource;
  private final Integer limit;
  private final SourceShape<List<I>> shape = new SourceShape<>(outlet);

  public BatchSource(BiFunction<Integer, Integer, CompletionStage<List<I>>> objectSource, Integer limit) {
    this.objectSource = objectSource;
    this.limit = limit;
  }

  @Override
  public SourceShape<List<I>> shape() {
    return shape;
  }


  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) {
    return new GraphStageLogicWithLoggingAndHandler<>(shape, outlet, objectSource, limit);
  }


  public static class GraphStageLogicWithLoggingAndHandler<I> extends GraphStageLogicWithLogging implements OutHandler {
    private final Outlet<List<I>> outlet;
    private final BiFunction<Integer, Integer, CompletionStage<List<I>>> objectSource;
    private final Integer limit;
    private Integer offset = 0;

    GraphStageLogicWithLoggingAndHandler(Shape _shape, Outlet<List<I>> outlet, BiFunction<Integer, Integer, CompletionStage<List<I>>> objectSource, Integer limit) {
      super(_shape);
      this.outlet = outlet;
      this.objectSource = objectSource;
      this.limit = limit;
      setHandler(outlet, this);
    }

    @Override
    public void onPull() throws Exception {
      AsyncCallback<List<I>> callback = createAsyncCallback((Procedure<List<I>>) list -> {
        push(outlet, list);
        if (list.size() < limit)
          complete(outlet);
      });

      objectSource
          .apply(limit, offset)
          .thenApply(list -> {
            offset += list.size();
            return list;
          }).thenAccept(callback::invoke);
    }
  }


}
