package com.etl.util;

import java.io.*;
import java.util.*;

public class BatchReader {

    public static List<String> readBatch(BufferedReader br, int batchSize) throws IOException {
        List<String> batch = new ArrayList<>();
        String line;

        while (batch.size() < batchSize && (line = br.readLine()) != null) {
            batch.add(line);
        }

        return batch;
    }
}