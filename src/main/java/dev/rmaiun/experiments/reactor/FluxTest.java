package dev.rmaiun.experiments.reactor;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FluxTest {

  private static final Logger log = LoggerFactory.getLogger(FluxTest.class);

  public static void main(String[] args) throws InterruptedException {
    var start = System.currentTimeMillis();
    Flux.fromIterable(Arrays.asList(5, 1, 2, 4, 7))
        .concatMap(e -> Mono.delay(Duration.of(e, ChronoUnit.SECONDS))
            .doOnNext(bla -> log.info("processing {}", e))
            .flatMap(bla -> Mono.just(e)))
        .doFinally(bla -> log.info("total={}", (System.currentTimeMillis() - start) / 1000))
        .subscribe(x -> log.info("result {}", x));
    Thread.sleep(30_000);
  }
}
