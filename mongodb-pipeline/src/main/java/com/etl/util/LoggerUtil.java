package com.etl.util;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class LoggerUtil {

    private static final String FILE_PATH = "output.txt";

    public static void log(String message) {
        try (PrintWriter out = new PrintWriter(new FileWriter(FILE_PATH, true))) {
            out.println(message);
        } catch (IOException e) {
            System.out.println("Logging failed: " + e.getMessage());
        }
    }

    public static void clear() {
        try (PrintWriter out = new PrintWriter(new FileWriter(FILE_PATH))) {
            out.print("");
        } catch (IOException e) {
            System.out.println("Clear log failed");
        }
    }
}