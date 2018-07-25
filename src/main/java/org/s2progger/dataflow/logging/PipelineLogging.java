package org.s2progger.dataflow.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.rolling.RollingFileAppender;

import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import org.s2progger.dataflow.config.PipelineLoggingConfig;
import org.slf4j.LoggerFactory;

public class PipelineLogging {

    public static Logger createLogger(String pipelineName, PipelineLoggingConfig config) {
        LoggerContext logCtx = (LoggerContext)LoggerFactory.getILoggerFactory();

        Logger log = logCtx.getLogger(pipelineName);
        log.setAdditive(false);
        log.setLevel(Level.INFO);

        if (config != null && config.getConsole() != null) {
            PatternLayoutEncoder consoleLogEncoder = new PatternLayoutEncoder();
            consoleLogEncoder.setContext(logCtx);
            consoleLogEncoder.setPattern(config.getConsole().getLogPattern());
            consoleLogEncoder.start();

            ConsoleAppender<ILoggingEvent> logConsoleAppender = new ConsoleAppender<>();
            logConsoleAppender.setContext(logCtx);
            logConsoleAppender.setName("console");
            logConsoleAppender.setEncoder(consoleLogEncoder);
            logConsoleAppender.start();

            log.addAppender(logConsoleAppender);
        }

        if (config != null && config.getFile() != null) {
            PatternLayoutEncoder fileLogEncoder = new PatternLayoutEncoder();
            fileLogEncoder.setContext(logCtx);
            fileLogEncoder.setPattern(config.getFile().getLogPattern());
            fileLogEncoder.start();

            RollingFileAppender<ILoggingEvent> logFileAppender = new RollingFileAppender<>();
            logFileAppender.setContext(logCtx);
            logFileAppender.setName("file");
            logFileAppender.setEncoder(fileLogEncoder);
            logFileAppender.setAppend(true);
            logFileAppender.setFile("logs/" + pipelineName.toLowerCase().replace(" ", "_") + "-pipeline.log");

            TimeBasedRollingPolicy logFilePolicy = new TimeBasedRollingPolicy();
            logFilePolicy.setContext(logCtx);
            logFilePolicy.setParent(logFileAppender);
            logFilePolicy.setFileNamePattern("logs/" + pipelineName.toLowerCase().replace(" ", "_") + "-pipeline.%d{yyyy-MM-dd}.log");
            logFilePolicy.setMaxHistory(50);
            logFilePolicy.start();

            logFileAppender.setRollingPolicy(logFilePolicy);
            logFileAppender.start();

            log.addAppender(logFileAppender);
        }

        return log;
    }
}
