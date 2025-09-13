package com.example.s3.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Small helpers for locating resource files and creating test payloads.
 */
public final class FileUtil {
    private FileUtil() {}

    public static Path resource(String relative) {
        // returns project-relative path for ease of local testing
        return Paths.get("src/main/resources/s3").resolve(relative);
    }

    public static Path ensureTestFile(String name, int sizeBytes) throws IOException {
        Path p = resource(name);
        if (Files.exists(p) && Files.size(p) == sizeBytes) {
            return p;
        }
        // create directories if missing
        if (!Files.exists(p.getParent())) {
            Files.createDirectories(p.getParent());
        }
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; i++) data[i] = (byte) (i % 127);
        try (var out = Files.newOutputStream(p)) {
            int written = 0;
            while (written < sizeBytes) {
                int toWrite = Math.min(data.length, sizeBytes - written);
                out.write(data, 0, toWrite);
                written += toWrite;
            }
        }
        return p;
    }
}