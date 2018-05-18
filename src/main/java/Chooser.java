import akka.stream.Attributes;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.UniformFanOutShape;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;

import java.util.function.Function;

public class Chooser<I, O> extends GraphStage<UniformFanOutShape<I, O>> {

  private final Inlet<I> inlet = Inlet.create("Chooser.in.0");
  private final Outlet<O> out0 = Outlet.create("Chooser.out.0");
  private final Outlet<O> out1 = Outlet.create("Chooser.out.1");
  private final UniformFanOutShape<I, O> shape = new UniformFanOutShape<I, O>(inlet, new Outlet[]{out0, out1});
  private final Function<I, Boolean> filter;
  private final Function<I, O> transformer;

  public Chooser(Function<I, Boolean> filter, Function<I, O> transformer) {
    this.filter = filter;
    this.transformer = transformer;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
    return new GraphStageLogic(shape) {
      {
        setHandler(inlet, new AbstractInHandler() {
          @Override
          public void onPush() {
            I input = grab(inlet);
            O data = transformer.apply(input);
            if (filter.apply(input))
              push(out0, data);
            else
              push(out1, data);
          }
        });
        setHandler(out0, new AbstractOutHandler() {
          @Override
          public void onPull() {
            if (!isClosed(inlet) && !hasBeenPulled(inlet))
              pull(inlet);
          }
        });

        setHandler(out1, new AbstractOutHandler() {
          @Override
          public void onPull() {
            if (!isClosed(inlet) && !hasBeenPulled(inlet))
              pull(inlet);
          }
        });
      }
    };
  }

  @Override
  public UniformFanOutShape<I, O> shape() {
    return shape;
  }
}
