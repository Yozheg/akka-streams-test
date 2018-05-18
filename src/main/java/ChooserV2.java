import akka.stream.*;
import akka.stream.stage.*;
import io.daydev.common.functional.Either;

public class ChooserV2<A, B> extends GraphStage<FanOutShape2<Either<A, B>, A, B>> {

    private final Inlet<Either<A, B>> in = Inlet.create("Chooser.in.0");
    private final Outlet<A> out0 = Outlet.create("Chooser.out.0");
    private final Outlet<B> out1 = Outlet.create("Chooser.out.1");

    private final FanOutShape2<Either<A, B>, A, B> shape = new FanOutShape2<>(in, out0, out1);

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
        return new GraphStageLogicWithLoggingAndHandlers<>(shape, in, out0, out1);
    }

    @Override
    public FanOutShape2<Either<A, B>, A, B> shape() {
        return shape;
    }

    private static class GraphStageLogicWithLoggingAndHandlers<A, B> extends GraphStageLogicWithLogging implements InHandler, OutHandler {

        private final Inlet<Either<A, B>> in;
        private final Outlet<A> out0;
        private final Outlet<B> out1;

        GraphStageLogicWithLoggingAndHandlers(FanOutShape2<Either<A, B>, A, B> _shape, Inlet<Either<A, B>> in, Outlet<A> out0, Outlet<B> out1) {
            super(_shape);
            this.in = in;
            this.out0 = out0;
            this.out1 = out1;

            setHandler(in, this);
            setHandler(out0, this);
            setHandler(out1, this);
        }

        @Override
        public void preStart() throws Exception {
            super.preStart();
            log().info("Prestart ChooserV2");
            pull(in);
        }

        @Override
        public void onPull() throws Exception {
            log().info("onPull ChooserV2");
            if (!isClosed(in) && !hasBeenPulled(in)) {
                pull(in);
            }
        }

        @Override
        public void onPush() throws Exception {
            log().info("onPush ChooserV2");
            Either<A, B> msg = grab(in);
            if (msg.isLeft()) {
                emit(out0, msg.getLeft());
            } else {
                emit(out1, msg.getRight());
            }

            pull(in);
        }
    }
}
