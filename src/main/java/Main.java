import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.japi.function.Function;
import akka.stream.*;
import akka.stream.javadsl.*;
import io.daydev.common.functional.Either;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

public class Main {


  private final Function<Throwable, Supervision.Directive> decider = exc -> Supervision.resume();

  private final ActorSystem system = ActorSystem.create("StreamsExamples");
  private final Materializer mat = ActorMaterializer.create(
      ActorMaterializerSettings.create(system).withSupervisionStrategy(decider),
      system);

  public static void main(String[] args) {
    new Main().run();
  }

  private void run() {
    //demoCombine();
    // demo1();
    demo3();
  }

  void demo3() {
    //Пишем лог
    final Flow<Task, Task, NotUsed> finalFlow = Flow.<Task>create()
        .map(task -> {
          System.out.println("LOGGING " + task.getName());
          return task;
        });
    //Проверку делаем
    final Flow<Task, Either<Task, Task>, NotUsed> checkFlow = Flow.<Task>create()
        .mapAsync(1, task -> CompletableFuture.supplyAsync(() -> Integer.parseInt(task.getName()) % 2 == 0).thenApply(result -> result ? Either.left(task) : Either.right(task)));

    //Делаем работу
    final Flow<Task, Task, NotUsed> workFlow = Flow.<Task>create().map(task -> {
      System.out.println("WORKING " + task.getName());
      return task;
    });

    final ChooserV2<Task, Task> chooser = new ChooserV2<>();

    //Делаем проверку, если не ок - логируемся и выходим, если ок - делаем работу, логируемся и выходим
    final Flow<Task, Task, NotUsed> flow = Flow.fromGraph(GraphDSL.create(
        b -> {
          FanOutShape2<Either<Task, Task>, Task, Task> choose = b.add(chooser);
          final UniformFanInShape<Task, Task> merge = b.add(Merge.create(2));
          final FlowShape<Task, Either<Task, Task>> checker = b.add(checkFlow);
          final FlowShape<Task, Task> logger = b.add(finalFlow);
          final FlowShape<Task, Task> wFlow = b.add(workFlow);

          b.from(checker).toInlet(choose.in());
          b.from(choose.out0()).toInlet(wFlow.in());
          b.from(wFlow.out()).toInlet(merge.in(0));
          b.from(choose.out1()).toInlet(merge.in(1));
          b.from(merge.out()).toInlet(logger.in());
          return FlowShape.of(checker.in(), logger.out());
        }
    ));

    Source<Task, NotUsed> tasks = Source.range(1, 10).map(String::valueOf).map(Task::new);
    tasks.via(flow).to(Sink.ignore()).run(mat);

  }

  void demo1() {
    Source<Task, NotUsed> tasks = Source.range(1, 10).map(String::valueOf).map(Task::new);
    Sink<Task, CompletionStage<Done>> sink = Sink.<Task>foreach(task -> System.out.println(task.getName()));
    MyJob myJob = new MyJob();
    tasks.via(myJob.getFlow()).to(sink).run(mat);
  }


  private void demoCombine() {
    AtomicReference<Integer> i = new AtomicReference<>(10);
    AtomicReference<Integer> j = new AtomicReference<>(100);
    AtomicReference<Integer> k = new AtomicReference<>(1000);


    //Допустим тут у нас 3 задачи - егаис, что-то там еще  и еще. У них мы выставляем рейты исполнения (могут быть сильно разными)
    Source<String, Cancellable> srcScheduler1 = Source.tick(Duration.ofMillis(100), Duration.ofSeconds(1), 0).map(t -> "Scheduled" + i.getAndSet(i.get() + 1).toString());
    Source<String, Cancellable> srcScheduler2 = Source.tick(Duration.ofMillis(100), Duration.ofSeconds(2), 0).map(t -> "Scheduled" + j.getAndSet(j.get() + 1).toString());
    Source<String, Cancellable> srcScheduler3 = Source.tick(Duration.ofMillis(100), Duration.ofSeconds(3), 0).map(t -> "Scheduled" + k.getAndSet(k.get() + 1).toString());
    //Мерджим их в один источник - без приоритета - нам похер что выполняется лишь бы выполнялось
    Source<String, NotUsed> source = Source.combine(srcScheduler1, srcScheduler2, Collections.singletonList(srcScheduler3), mmm -> Merge.create(3));
    //Далее у нас есть пользовательский источник
    Source<String, Cancellable> srcUser = Source.tick(Duration.ofMillis(10), Duration.ofMillis(400), 0).map(nil -> "UserTask");
    //Выше - лучше
    int[] priorities = new int[]{1, 100};
    Source.combine(source, srcUser, Collections.emptyList(), integer -> MergePrioritized.create(priorities)).to(Sink.foreach(System.out::println)).run(mat);

  }

}
