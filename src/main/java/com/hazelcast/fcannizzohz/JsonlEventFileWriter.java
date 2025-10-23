package com.hazelcast.fcannizzohz;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

public class JsonlEventFileWriter {

    private final BufferedWriter writer;

    public JsonlEventFileWriter(String outputDir) throws IOException {
        if (outputDir == null || outputDir.isBlank()) {
            this.writer = null;
            return;
        }

        // Ensure parent directories exist
        Path dirPath = Paths.get(outputDir);
        Files.createDirectories(dirPath);

        // Create ISO timestamp for filename
        String timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now())
                                                        .replace(":", "-"); // Safe for file systems

        Path filePath = dirPath.resolve("output_" + timestamp + ".jsonl");

        // Open file for appending
        this.writer = new BufferedWriter(new FileWriter(filePath.toFile(), true));
    }

    public void appendEvent(String jsonEvent) throws IOException {
        if (writer == null) return;
        if (jsonEvent == null || jsonEvent.isBlank()) return;

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(jsonEvent);

        if (root.isArray()) {
            for (JsonNode node : root) {
                writer.write(mapper.writeValueAsString(node));
                writer.newLine();
            }
        } else {
            // Just a single JSON object
            writer.write(mapper.writeValueAsString(root));
            writer.newLine();
        }
        writer.flush();
    }

    public void close() throws IOException {
        if (writer != null) writer.close();
    }
}