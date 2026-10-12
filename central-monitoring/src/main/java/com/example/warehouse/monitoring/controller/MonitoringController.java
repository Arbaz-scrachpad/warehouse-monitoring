package com.example.warehouse.monitoring.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;

@RestController
public class MonitoringController {
    @GetMapping("/api/v1/status")
    public Mono<Map<String,Object>> status(){
        return Mono.just(Map.of(
                "service","central-monitoring",
                "status","UP",
                "timestamp", Instant.now().toString()
        ));
    }
}
