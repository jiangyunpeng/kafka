package org.apache.kafka;

import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


public class SourceLogger {

    private static final String SPLIT = " ";
    private static final String SPLIT1 = " - ";
    private static final String TAB = "  ";
    private static final BlockingQueue<String> logQueue = new LinkedBlockingDeque<>();
    private static final AtomicLong seq = new AtomicLong();
    private static final ThreadLocal<Context> localContext = new ThreadLocal<>();
    private static File logFile;
    private static BufferedWriter writer;
    private static Filter filter = (log) -> {
        return log.contains("2kafka-coordinator-heartbeat-thread");
    };

    static {
        try {
            init();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    interface Filter {
        boolean filter(String log);
    }

    private static void init() throws FileNotFoundException {
        File logDir = new File(System.getProperty("user.dir"), "logs");
        logDir.mkdirs();
        logFile = new File(logDir, "kafka.log");
        System.out.println("logFile path: " + logFile);

        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(logFile)));
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    write(true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        t.setName("sourceLogger-write-thread");
        t.setDaemon(true);
        t.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                write(false);
                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }

    private static void write(boolean block) throws Exception {
        List<String> parcel = new ArrayList<String>();
        if (block) {
            String message = logQueue.take();
            parcel.add(message);
        }
        logQueue.drainTo(parcel);
        for (String log : parcel) {
            writer.write(log + "\r\n");
        }
        writer.flush();
    }

    public synchronized static void info(String message, Object... args) {
        info(null, message, args);
    }


    private static void write(String log) {
        if (filter != null && filter.filter(log)) {
            return;
        }
        //logQueue.add(log);
        System.out.println(log);
    }

    public synchronized static void info(Class type, String message, Object... args) {
        write(format(type, message, args));
    }

    public synchronized static void error(Class type, String message, Object... args) {
        write(format(type, message, args));
    }

    public synchronized static void start(Class type, String message, Object... args) {
        Context context = localContext.get();
        if (context == null) {
            context = Context.create();
            localContext.set(context);
        }
        context.start();
        info(type, message, args);
    }

    public synchronized static void end(Class type, String message, Object... args) {
        Context context = localContext.get();
        info(type, message, args);
        if (context.end())
            localContext.remove();
    }


    private static String format(Class type, String message, Object... args) {
        StringBuilder sb = new StringBuilder();
        sb.append(seq.getAndIncrement());
        sb.append(SPLIT);
        //spanId
        sb.append(formatSpanId());
        sb.append(SPLIT);
        //sb.append(formatIndent());
        sb.append(formatDatetime());
        sb.append(SPLIT);
        sb.append(formatThread());
        sb.append(SPLIT);
        if (type != null) {
            sb.append(formatClass(type));
            sb.append(SPLIT1);
        }
        FormattingTuple ft = MessageFormatter.arrayFormat(message, args);
        sb.append(ft.getMessage());
        return sb.toString();
    }

    private static String formatIndent() {
        StringBuilder sb = new StringBuilder();
        Context ctx = null;
        if ((ctx = localContext.get()) != null) {
            for (int i = 0; i < ctx.count.get(); ++i) {
                sb.append(TAB);
            }
        }
        return sb.toString();
    }

    private static String formatSpanId() {
        if (localContext.get() != null) {
            Context ctx = localContext.get();
            return ctx.traceId + "-" + ctx.count;
        } else {
            return "-";
        }
    }

    private static String formatDatetime() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
    }

    private static String formatThread() {
        return "[" + Thread.currentThread().getName() + "]";
    }

    private static String formatClass(Class type) {
        return type.getSimpleName();
    }

    private static class Context {
        public String traceId;
        private AtomicInteger count = new AtomicInteger();

        public static Context create() {
            Context context = new Context();
            context.traceId = IdGenerator.generate();
            return context;
        }

        public void start() {
            count.incrementAndGet();
        }

        public boolean end() {
            return count.decrementAndGet() == 0;
        }
    }

    static class IdGenerator {
        private static final AtomicLong count = new AtomicLong();

        public static String generate() {
            long id = System.currentTimeMillis() + count.getAndIncrement();
            StringBuilder sb = new StringBuilder();
            sb.append(byteToString((byte) (id >>> 40 & 0xff)));
            sb.append(byteToString((byte) (id >>> 32 & 0xff)));
            sb.append(byteToString((byte) (id >>> 24 & 0xff)));
            sb.append(byteToString((byte) (id >>> 16 & 0xff)));
            sb.append(byteToString((byte) (id >>> 8 & 0xff)));
            sb.append(byteToString((byte) (id & 0xff)));
            return sb.toString();
        }

        private static String byteToString(byte b) {
            char[] chars = "0123456789abcdef".toCharArray();
            StringBuilder sb = new StringBuilder();
            sb.append(chars[(b & 0x0f0) >> 4]);
            sb.append(chars[b & 0x0f]);
            return sb.toString();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        //info(SourceLogger.class, "The configuration 'value.serializer' was supplied ");
        for (int i = 0; i < 10; ++i) {
            System.out.println(IdGenerator.generate());
            Thread.sleep(1000);
        }
    }
}
