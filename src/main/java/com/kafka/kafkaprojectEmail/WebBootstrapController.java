package com.kafka.kafkaprojectEmail;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class WebBootstrapController {

    @GetMapping("/health")
    public String health() {
        return "UP";
    }
}

