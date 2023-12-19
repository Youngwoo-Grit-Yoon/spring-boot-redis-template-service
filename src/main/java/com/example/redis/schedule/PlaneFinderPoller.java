package com.example.redis.schedule;

import com.example.redis.domain.Aircraft;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Set;

@EnableScheduling
@Component
public class PlaneFinderPoller {
    private final WebClient client = WebClient.create("http://localhost:7634/aircraft");

    private final RedisConnectionFactory connectionFactory;
    private final RedisOperations<String, Aircraft> redisOperations;

    PlaneFinderPoller(RedisConnectionFactory connectionFactory, RedisOperations<String, Aircraft> redisOperations) {
        this.connectionFactory = connectionFactory;
        this.redisOperations = redisOperations;
    }

    @Scheduled(fixedRate = 1000)
    private void pollPlanes() {
        System.out.println("Starting Aircraft Polling...");
        connectionFactory.getConnection().serverCommands().flushDb();

        // Redis에 데이터 저장
        client.get()
                .retrieve()
                .bodyToFlux(Aircraft.class)
                .filter(plane -> !plane.getReg().isEmpty())
                .toStream()
                .forEach(ac -> redisOperations.opsForValue().set(ac.getReg(), ac));

        // Redis에 저장된 데이터 키 조회
        Set<String> aircraftKeys = redisOperations.opsForValue()
                .getOperations()
                .keys("*");

        // Redis에 데이터 키를 조회하여 데이터 출력
        if (aircraftKeys != null) {
            for (String aircraftKey : aircraftKeys) {
                System.out.println(redisOperations.opsForValue().get(aircraftKey));
            }
        }
    }
}
