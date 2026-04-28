package com.etl.parser;

import com.etl.model.LogRecord;
import java.util.regex.*;

public class LogParser {

    private static final Pattern pattern = Pattern.compile(
        "^(\\S+) \\S+ \\S+ \\[([^\\]]+)\\] \"(.*?)\" (\\d{3}) (\\S+)"
    );

    public static LogRecord parse(String line) {
        Matcher m = pattern.matcher(line);

        if (!m.find()) return null;

        try {
            LogRecord r = new LogRecord();

            r.host = m.group(1);

            String timestamp = m.group(2);
            String[] tsParts = timestamp.split(":");
            if (tsParts.length < 2) return null;

            r.date = tsParts[0];
            r.hour = Integer.parseInt(tsParts[1]);

            String request = m.group(3);
            String[] reqParts = request.split(" ");
            if (reqParts.length < 3) return null;

            r.method = reqParts[0];
            r.path = reqParts[1];
            r.protocol = reqParts[2];

            r.status = Integer.parseInt(m.group(4));

            String bytesStr = m.group(5);
            r.bytes = bytesStr.equals("-") ? 0 : Long.parseLong(bytesStr);

            return r;

        } catch (Exception e) {
            return null;
        }
    }
}