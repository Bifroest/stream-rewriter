package io.bifroest.stream_rewriter.netty;

import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;

import org.apache.logging.log4j.LogManager;

public class Log4J2LoggerFactory extends InternalLoggerFactory {

    @Override
    public InternalLogger newInstance(String name) {
        return new Log4J2Logger(LogManager.getLogger(name));
    }
}
