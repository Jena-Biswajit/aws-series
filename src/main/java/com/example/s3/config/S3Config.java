package com.example.s3.config;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public final class S3Config {

    private S3Config() {}

    /**
     * Build a reusable S3Client. Uses DefaultCredentialsProvider so it picks up
     * ~/.aws/credentials or environment variables automatically.
     */
    public static S3Client s3Client(Region region) {
        return S3Client.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }
}