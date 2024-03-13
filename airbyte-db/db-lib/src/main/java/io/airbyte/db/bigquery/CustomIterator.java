package io.airbyte.db.bigquery;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class CustomIterator<JsonNode> implements Iterator<JsonNode> {

  private final List<Future<Stream<JsonNode>>> futures;
  private final LinkedList<JsonNode> responseData;
  private int currentIndex;

  public CustomIterator(List<Future<Stream<JsonNode>>> futures) {
    this.futures = futures;
    this.responseData =  new LinkedList<>();
    this.currentIndex = 0;
  }

  @Override
  public boolean hasNext() {
      if(responseData.isEmpty()) {
          // if there is no index left to iterate
          if(currentIndex>=futures.size()) return false;
          try {
              Instant start  = Instant.now();
              futures.get(currentIndex).get().forEach(responseData::add);
              System.out.println("time taken to get future results:  "+java.time.Duration.between(start,Instant.now()).toSeconds());
              // if future return empty list
              if(responseData.isEmpty()) return false;
              currentIndex++;
          } catch (InterruptedException e) {
              throw new RuntimeException(e);
          } catch (ExecutionException e) {
              throw new RuntimeException(e);
          }
      }
    return true;
  }

  @Override
  public JsonNode next() {
      return responseData.removeFirst();
  }
}
