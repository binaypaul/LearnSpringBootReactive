package com.binaypaul.learnspringbootreactive.concepts;

import java.time.Duration;
import java.util.List;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

public class FluxTest {
    private Flux<String> testFlux() {
        // var list = Arrays.asList("Flux", "Python", "Rust", "C++");
        // return Flux.fromIterable(list);
        return Flux.just("Java", "Python", "Rust", "C++");
    }

    private Flux<String> testMap() {
        return Flux.just("Java", "Python", "Rust", "C++").map(data -> data.toUpperCase()).block());
    }

    private Flux<String> testFlatMap() {
        return Flux.just("Java", "Python", "Rust", "C++").flatMap(data -> Mono.just(data.toUpperCase()));
    }

    private Flux<String> testFlatMapMany() {
        return Mono.just("Java").flatMapMany(data -> Flux.just(data.toUpperCase(), "Python", "Rust", "C++"));
    }

    private Flux<String> testDelayAndSkip() {
        var delayedFlux = Flux.just("Java", "Python", "Rust", "C++").delayElements(Duration.ofSeconds(1));
        return delayedFlux.skip(Duration.ofMillis(2020));
    }

    private Flux<Integer> testSkipUntill() {
        return Flux.range(1, 10).skipUntil(i -> i == 5);
    }

    private Flux<Integer> testSkipWhile() {
        return Flux.range(1, 10).skipWhile(i -> i <= 5);
    }

    private Flux<Integer> testConcat() {
        var flux1 = Flux.range(1, 10);
        var flux2 = Flux.range(101, 10);
        return Flux.concat(flux1, flux2, Mono.just(10_000));
    }

    private Flux<Integer> testMerge() {
        var flux1 = Flux.range(1, 10)
                .delayElements(Duration.ofMillis(500));
        var flux2 = Flux.range(101, 10)
                .delayElements(Duration.ofMillis(500));
        return Flux.merge(flux1, flux2, Mono.just(10_000));
    }

    private Flux<Tuple2<Integer, Integer>> testZip() {
        var flux1 = Flux.range(1, 10)
                .delayElements(Duration.ofMillis(500));
        var flux2 = Flux.range(101, 10)
                .delayElements(Duration.ofMillis(500));
        return Flux.zip(flux1, flux2);
    }

    private Mono<List<Integer>> testCollection() {
        var flux1 = Flux.range(1, 10);
        return flux1.collectList();
    }

    public static void main(String[] args) throws InterruptedException {
        FluxTest fluxPublisher = new FluxTest();
        // System.out.println("\ntestFlux");
        // fluxPublisher.testFlux().subscribe(System.out::println);
        System.out.println("\ntestMap");
        fluxPublisher.testMap().subscribe(System.out::println);
        System.out.println("\ntestFlatMap");
        fluxPublisher.testFlatMap().subscribe(System.out::println);
        // System.out.println("\ntestFlatMapMany");
        // fluxPublisher.testFlatMapMany().subscribe(System.out::println);
        // System.out.println("\ntestDelayAndSkip");
        // fluxPublisher.testDelayAndSkip().doOnNext(System.out::println).blockLast();
        // System.out.println("\ntestSkipUntill");
        // fluxPublisher.testSkipUntill().subscribe(System.out::println);
        // System.out.println("\ntestSkipWhile");
        // fluxPublisher.testSkipWhile().subscribe(System.out::println);
        // System.out.println("\ntestConcat");
        // fluxPublisher.testConcat().subscribe(System.out::println);
        // System.out.println("\ntestMerge");
        // fluxPublisher.testMerge().doOnNext(System.out::println).blockLast();
        // System.out.println("\ntestZip");
        // fluxPublisher.testZip().doOnNext(System.out::println).blockLast();
        // System.out.println("\ntestCollection");
        // fluxPublisher.testCollection().doOnNext(System.out::println).block();
        System.out.println("\nCompleted..!");
    }
}
