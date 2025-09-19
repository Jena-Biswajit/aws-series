package com.example.app.controller;


import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

@RestController
public class HelloWorldController {

    @GetMapping("/health")
    public String health() {
        // Return current date & time in ISO-8601 format
        return LocalDateTime.now().toString();
    }
}