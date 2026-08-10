package com.binaypaul.learnspringbootreactive.concepts;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class MonoTest {
    private Mono<String> testMono() {
        // return Mono.just("Java").log(); // shows the Publisher-Subscriber flow.
        // return Mono.justOrEmpty("Java"); //be able to return data or null.
        // return Mono.empty(); //return null.
        return Mono.just("Mono");
    }

    public static void main(String[] args) {
        MonoTest monoPublisher = new MonoTest();

        monoPublisher.testMono().subscribe(data -> System.out.println(data));
    }
}
