import akka.NotUsed;
import akka.stream.javadsl.Flow;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class MyJob {

  public Flow<Task, Task, NotUsed> getFlow() {
    return Flow.<Task>create().mapAsync(1, task -> map(task)
        .handle((tsk, throwable) -> {
          if (throwable != null) {
            System.out.println("Got error");
            return task;
          }
          return tsk;
        }));
  }

  private CompletionStage<Task> map(Task task) {
    return CompletableFuture.supplyAsync(() -> {
      System.out.println("inside the flow " + task.getName());
      if (task.getName().equalsIgnoreCase("5"))
        throw new RuntimeException("kek");
      return task;
    });
  }

}




/*     mapAsync(msg => {
          execFuture.map(result => {
              (msg, result)
    }
}*/
