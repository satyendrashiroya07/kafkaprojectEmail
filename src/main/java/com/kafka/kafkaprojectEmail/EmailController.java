package com.kafka.kafkaprojectEmail;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EmailController {

        @GetMapping("/response/200")
        public ResponseEntity<String> test() {
            return ResponseEntity.ok("Success");
        }
}
