//package com.example.s3.service;
//
//import com.example.s3.config.S3Config;
//import com.example.s3.exception.S3OperationException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import software.amazon.awssdk.core.sync.RequestBody;
//import software.amazon.awssdk.services.s3.S3Client;
//import software.amazon.awssdk.services.s3.model.*;
//
//import java.io.RandomAccessFile;
//import java.nio.file.Path;
//import java.util.*;
//import java.util.concurrent.*;
//import java.util.stream.Collectors;
//
//public class MultipartUploadService {
//    private static final Logger log = LoggerFactory.getLogger(MultipartUploadService.class);
//
//    private final S3Client s3;
//    private final ExecutorService executor;
//
//    public MultipartUploadService(S3Client s3, int parallelism) {
//        this.s3 = s3;
//        this.executor = Executors.newFixedThreadPool(Math.max(2, parallelism));
//    }
//
//    /**
//     * Upload file in parts (chunkSizeBytes) in parallel, using CompletableFuture lambdas.
//     * This method reads parts safely and ensures abort on failure.
//     */
//    public void uploadInParts(String bucket, String key, Path filePath, int chunkSizeBytes) {
//        RandomAccessFile raf = null;
//        String uploadId = null;
//        try {
//            long contentLength = filePath.toFile().length();
//            if (contentLength == 0) {
//                throw new S3OperationException("File is empty: " + filePath);
//            }
//
//            // start multipart upload
//            CreateMultipartUploadResponse createResp = s3.createMultipartUpload(
//                    CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build()
//            );
//            uploadId = createResp.uploadId();
//            log.info("Started multipart upload uploadId={}", uploadId);
//
//            int partCount = (int) ((contentLength + chunkSizeBytes - 1) / chunkSizeBytes);
//            log.info("File size={}, parts={}", contentLength, partCount);
//
//            raf = new RandomAccessFile(filePath.toFile(), "r");
//
//            List<CompletableFuture<CompletedPart>> futures = new ArrayList<>(partCount);
//
//            for (int partNumber = 1; partNumber <= partCount; partNumber++) {
//                final int pn = partNumber;
//                final long offset = (long) (pn - 1) * chunkSizeBytes;
//                final int size = (int) Math.min(chunkSizeBytes, contentLength - offset);
//
//                RandomAccessFile finalRaf = raf;
//                RandomAccessFile finalRaf1 = raf;
//                String finalUploadId = uploadId;
//                CompletableFuture<CompletedPart> cf = CompletableFuture.supplyAsync(() -> {
//                    byte[] buffer = new byte[size];
//                    try {
//                        synchronized (finalRaf) {
//                            finalRaf1.seek(offset);
//                            finalRaf1.readFully(buffer);
//                        }
//                        UploadPartResponse upr = s3.uploadPart(
//                                UploadPartRequest.builder()
//                                        .bucket(bucket)
//                                        .key(key)
//                                        .uploadId(finalUploadId)
//                                        .partNumber(pn)
//                                        .contentLength((long) size)
//                                        .build(),
//                                RequestBody.fromBytes(buffer)
//                        );
//                        log.info("Uploaded part {} eTag={}", pn, upr.eTag());
//                        return CompletedPart.builder().partNumber(pn).eTag(upr.eTag()).build();
//                    } catch (Exception ex) {
//                        throw new CompletionException(new S3OperationException("Failed to upload part " + pn, ex));
//                    }
//                }, executor);
//
//                futures.add(cf);
//            }
//
//            // Wait for all parts
//            CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
//            all.join(); // propagate exceptions as CompletionException
//
//            // collect results, sort by part number
//            List<CompletedPart> completedParts = futures.stream()
//                    .map(CompletableFuture::join)
//                    .sorted(Comparator.comparingInt(CompletedPart::partNumber))
//                    .collect(Collectors.toList());
//
//            CompletedMultipartUpload multipart = CompletedMultipartUpload.builder().parts(completedParts).build();
//
//            // complete
//            s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
//                    .bucket(bucket)
//                    .key(key)
//                    .uploadId(uploadId)
//                    .multipartUpload(multipart)
//                    .build());
//
//            log.info("Completed multipart upload s3://{}/{} (parts={})", bucket, key, completedParts.size());
//        } catch (CompletionException cex) {
//            // Unwrap and abort upload if possible
//            log.error("Multipart upload failed: {}", cex.getCause().getMessage());
//            if (uploadId != null) {
//                try {
//                    s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
//                            .bucket(bucket)
//                            .key(key)
//                            .uploadId(uploadId)
//                            .build());
//                    log.info("Aborted multipart upload uploadId={}", uploadId);
//                } catch (Exception abortEx) {
//                    log.warn("Failed to abort multipart upload: {}", abortEx.getMessage());
//                }
//            }
//            throw (cex.getCause() instanceof RuntimeException) ? (RuntimeException) cex.getCause()
//                    : new S3OperationException("Multipart upload failed", cex.getCause());
//        } catch (Exception e) {
//            if (uploadId != null) {
//                try {
//                    s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
//                            .bucket(bucket)
//                            .key(key)
//                            .uploadId(uploadId)
//                            .build());
//                } catch (Exception abortEx) {
//                    log.warn("Failed to abort multipart upload after exception: {}", abortEx.getMessage());
//                }
//            }
//            throw new S3OperationException("Multipart upload failed", e);
//        } finally {
//            try {
//                if (raf != null) raf.close();
//            } catch (Exception ignore) {}
//            executor.shutdown();
//        }
//    }
//}

package com.example.s3.service;

import com.example.s3.config.S3Config;
import com.example.s3.exception.S3OperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Handles multipart uploads to S3 with parallel execution.
 * Provides both default (5MB) and custom part-size options.
 */
public class MultipartUploadService {
    private static final Logger log = LoggerFactory.getLogger(MultipartUploadService.class);

    // Default recommended part size for production (5 MB minimum for S3)
    private static final int PART_SIZE = 5 * 1024 * 1024;

    private final S3Client s3;
    private final ExecutorService executor;

    public MultipartUploadService(S3Client s3, int parallelism) {
        this.s3 = s3;
        this.executor = Executors.newFixedThreadPool(Math.max(2, parallelism));
    }

    /**
     * Overloaded method – uses default PART_SIZE (5MB).
     */
    public void uploadInParts(String bucket, String key, Path filePath) {
        uploadInParts(bucket, key, filePath, PART_SIZE);
    }

    /**
     * Upload file in parts (chunkSizeBytes) in parallel.
     * Supports custom part size for testing (e.g., 1MB) or production (>=5MB).
     */
    public void uploadInParts(String bucket, String key, Path filePath, int chunkSizeBytes) {
        RandomAccessFile raf = null;
        String uploadId = null;

        try {
            // ️ Enforce AWS S3 minimum part size (5 MB), except last part can be smaller
            if (chunkSizeBytes < PART_SIZE) {
                log.warn(" Requested part size {} is below S3 minimum (5 MB). Using default {} instead.",
                        chunkSizeBytes, PART_SIZE);
                chunkSizeBytes = PART_SIZE;
            }

            long contentLength = filePath.toFile().length();
            if (contentLength == 0) {
                throw new S3OperationException("File is empty: " + filePath);
            }

            // start multipart upload
            CreateMultipartUploadResponse createResp = s3.createMultipartUpload(
                    CreateMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build()
            );
            uploadId = createResp.uploadId();
            log.info("Started multipart upload uploadId={} for s3://{}/{}", uploadId, bucket, key);

            int partCount = (int) ((contentLength + chunkSizeBytes - 1) / chunkSizeBytes);
            log.info("File size={} bytes, chunkSize={} bytes, total parts={}", contentLength, chunkSizeBytes, partCount);

            raf = new RandomAccessFile(filePath.toFile(), "r");

            List<CompletableFuture<CompletedPart>> futures = new ArrayList<>(partCount);

            for (int partNumber = 1; partNumber <= partCount; partNumber++) {
                final int pn = partNumber;
                final long offset = (long) (pn - 1) * chunkSizeBytes;
                final int size = (int) Math.min(chunkSizeBytes, contentLength - offset);

                RandomAccessFile finalRaf = raf;
                String finalUploadId = uploadId;

                CompletableFuture<CompletedPart> cf = CompletableFuture.supplyAsync(() -> {
                    byte[] buffer = new byte[size];
                    try {
                        synchronized (finalRaf) {
                            finalRaf.seek(offset);
                            finalRaf.readFully(buffer);
                        }
                        UploadPartResponse upr = s3.uploadPart(
                                UploadPartRequest.builder()
                                        .bucket(bucket)
                                        .key(key)
                                        .uploadId(finalUploadId)
                                        .partNumber(pn)
                                        .contentLength((long) size)
                                        .build(),
                                RequestBody.fromBytes(buffer)
                        );
                        log.info("Uploaded part {} ({} bytes) eTag={}", pn, size, upr.eTag());
                        return CompletedPart.builder().partNumber(pn).eTag(upr.eTag()).build();
                    } catch (Exception ex) {
                        throw new CompletionException(new S3OperationException("Failed to upload part " + pn, ex));
                    }
                }, executor);

                futures.add(cf);
            }

            // Wait for all parts
            CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            all.join();

            // collect results, sort by part number
            List<CompletedPart> completedParts = futures.stream()
                    .map(CompletableFuture::join)
                    .sorted(Comparator.comparingInt(CompletedPart::partNumber))
                    .collect(Collectors.toList());

            CompletedMultipartUpload multipart = CompletedMultipartUpload.builder()
                    .parts(completedParts)
                    .build();

            // complete
            s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(multipart)
                    .build());

            log.info(" Completed multipart upload s3://{}/{} (parts={})", bucket, key, completedParts.size());

        } catch (CompletionException cex) {
            handleFailure(bucket, key, uploadId, cex.getCause());
        } catch (Exception e) {
            handleFailure(bucket, key, uploadId, e);
        } finally {
            try {
                if (raf != null) raf.close();
            } catch (Exception ignore) {}
            executor.shutdown();
        }
    }

    /**
     * Handle failure by aborting multipart upload.
     */
    private void handleFailure(String bucket, String key, String uploadId, Throwable cause) {
        log.error(" Multipart upload failed for s3://{}/{}: {}", bucket, key, cause.getMessage());
        if (uploadId != null) {
            try {
                s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .uploadId(uploadId)
                        .build());
                log.info("Aborted multipart upload uploadId={}", uploadId);
            } catch (Exception abortEx) {
                log.warn("Failed to abort multipart upload: {}", abortEx.getMessage());
            }
        }
        throw (cause instanceof RuntimeException) ? (RuntimeException) cause
                : new S3OperationException("Multipart upload failed", cause);
    }
}
