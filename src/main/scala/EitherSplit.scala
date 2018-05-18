import akka.stream._
import akka.stream.stage._

final case class EitherSplit[Left, Right]() extends GraphStage[FanOutShape2[Either[Left, Right], Left, Right]] {

  type EitherShape = FanOutShape2[Either[Left, Right], Left, Right]

  val in: Inlet[Either[Left, Right]] = Inlet("EitherSplit.in")
  val out0: Outlet[Left] = Outlet("EitherSplit.out0")
  val out1: Outlet[Right] = Outlet("EitherSplit.out1")

  val shape: EitherShape = new FanOutShape2(in, out0, out1)

  override def initialAttributes: Attributes = Attributes.name("EitherSplit")

  override def toString: String = "EitherSplit"

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      override def preStart(): Unit = {
        super.preStart()
        pull(in)
      }

      override def onPush(): Unit = {
        grab(in) match {
          case Left(left)   => emit(out0, left)
          case Right(right) => emit(out1, right)
        }
        pull(in)
      }

      override def onPull(): Unit = if (!isClosed(in) && !hasBeenPulled(in)) pull(in)

      setHandler(in, this)

      setHandler(out0, this)
      setHandler(out1, this)
    }
}
