package com.example.s3.service;

import com.example.s3.config.S3Config;
import com.example.s3.exception.S3OperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public class S3Service {
    private static final Logger log = LoggerFactory.getLogger(S3Service.class);
    private final S3Client s3;

    public S3Service(Region region) {
        this.s3 = S3Config.s3Client(region);
    }

    public List<Bucket> listBuckets() {
        ListBucketsResponse resp = s3.listBuckets();
        log.info("Found {} buckets", resp.buckets().size());
        return resp.buckets();
    }

    public boolean bucketExists(String bucket) {
        return listBuckets().stream().anyMatch(b -> b.name().equals(bucket));
    }

    public void createBucketIfNotExists(String bucket) {
        if (bucketExists(bucket)) {
            log.info("Bucket '{}' already exists -> skipping create", bucket);
            return;
        }
        try {
            CreateBucketRequest req = CreateBucketRequest.builder().bucket(bucket).build();
            s3.createBucket(req);
            log.info("Created bucket '{}'", bucket);
        } catch (S3Exception e) {
            throw new S3OperationException("Failed to create bucket: " + bucket, e);
        }
    }

    public void uploadFile(String bucket, String key, Path filePath) {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            s3.putObject(request, RequestBody.fromFile(filePath));
            log.info("Uploaded file {} -> s3://{}/{}", filePath, bucket, key);
        } catch (S3Exception e) {
            throw new S3OperationException("Upload failed for s3://"+bucket+"/"+key, e);
        }
    }

    public void downloadFile(String bucket, String key, Path destination) {
        try {
            GetObjectRequest req = GetObjectRequest.builder().bucket(bucket).key(key).build();
            s3.getObject(req, destination);
            log.info("Downloaded s3://{}/{} -> {}", bucket, key, destination);
        } catch (S3Exception e) {
            throw new S3OperationException("Download failed for s3://"+bucket+"/"+key, e);
        }
    }

    public boolean objectExists(String bucket, String key) {
        try {
            HeadObjectRequest req = HeadObjectRequest.builder().bucket(bucket).key(key).build();
            s3.headObject(req);
            return true;
        } catch (S3Exception e) {
            log.debug("HeadObject indicates object not present or not accessible: s3://{}/{}", bucket, key);
            return false;
        }
    }

    public void close() {
        s3.close();
    }
}