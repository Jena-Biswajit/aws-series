package com.example.s3.app;

import com.example.s3.exception.S3OperationException;
import com.example.s3.service.MultipartUploadService;
import com.example.s3.service.S3Service;
import com.example.s3.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.file.Path;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        // configure here
        Region region = Region.AP_SOUTHEAST_2; // Sydney; change if required
        String bucket = "biswa-cross-account-dest"; // update if needed

        S3Service s3Service = new S3Service(region);
        try {
            // ensure bucket exists
            s3Service.createBucketIfNotExists(bucket);

            // upload small sample
            Path sample = FileUtil.resource("sample.txt");
            if (!sample.toFile().exists()) {
                // create tiny sample if missing
                FileUtil.ensureTestFile("sample.txt", 1024);
            }
            s3Service.uploadFile(bucket, "sample.txt", sample);

            // ensure big test file (~10 MB)
            Path big = FileUtil.resource("bigfile.bin");
            if (!big.toFile().exists() || big.toFile().length() < 10 * 1024 * 1024L) {
                log.info("Creating 10MB test file...");
                FileUtil.ensureTestFile("bigfile.bin", 10 * 1024 * 1024);
            }

            // multipart upload using the same s3 client
            S3Client s3Client = com.example.s3.config.S3Config.s3Client(region);
            MultipartUploadService multipart = new MultipartUploadService(s3Client, 5);
            multipart.uploadInParts(bucket, "bigfile.bin", big, 1024 * 1024); // 1MB parts

            // verify existence
            boolean ok = s3Service.objectExists(bucket, "bigfile.bin");
            log.info("bigfile.bin exists on S3: {}", ok);

        } catch (S3OperationException ex) {
            log.error("S3 operation failed: {}", ex.getMessage(), ex);
            System.exit(2);
        } catch (Exception ex) {
            log.error("Unexpected error: {}", ex.getMessage(), ex);
            System.exit(3);
        } finally {
            s3Service.close();
        }
    }
}