package com.etl.parser;

import java.util.regex.*;
import com.etl.model.LogRecord;

public class LogParser {

    private static final Pattern PATTERN = Pattern.compile(
            "^(\\S+) \\S+ \\S+ \\[([^\\]]+)\\] \"(.*?)\" (\\d{3}) (\\S+)");

    public static LogRecord parse(String line) {

        try {

            Matcher m = PATTERN.matcher(line);

            if (!m.find())
                return null;

            LogRecord r = new LogRecord();

            r.host = m.group(1);

            String timestamp = m.group(2);
            String[] t = timestamp.split(":");

            if (t.length < 2)
                return null;

            r.date = t[0];
            r.hour = Integer.parseInt(t[1]);

            String request = m.group(3);
            String[] req = request.split(" ");

            if (req.length < 3)
                return null;

            r.method = req[0];
            r.path = req[1];
            r.protocol = req[2];

            r.status = Integer.parseInt(m.group(4));

            String bytesStr = m.group(5);

            if (bytesStr.equals("-"))
                r.bytes = 0;
            else
                r.bytes = Long.parseLong(bytesStr);

            return r;

        } catch (Exception e) {
            return null;
        }

    }

}