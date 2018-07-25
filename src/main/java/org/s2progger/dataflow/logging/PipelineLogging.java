package org.s2progger.dataflow.logging;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.RollingFileAppender;

import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import org.s2progger.dataflow.config.PipelineLoggingConfig;
import org.slf4j.LoggerFactory;

public class PipelineLogging {

    public static Logger createLogger(String pipelineName, PipelineLoggingConfig config) {
        LoggerContext logCtx = (LoggerContext)LoggerFactory.getILoggerFactory();

        PatternLayoutEncoder consoleLogEncoder = new PatternLayoutEncoder();
        consoleLogEncoder.setContext(logCtx);
        consoleLogEncoder.setPattern(config.getConsole().getLogPattern());
        consoleLogEncoder.start();

        ConsoleAppender logConsoleAppender = new ConsoleAppender();
        logConsoleAppender.setContext(logCtx);
        logConsoleAppender.setName("console");
        logConsoleAppender.setEncoder(consoleLogEncoder);
        logConsoleAppender.start();

        PatternLayoutEncoder fileLogEncoder = new PatternLayoutEncoder();
        fileLogEncoder.setContext(logCtx);
        fileLogEncoder.setPattern(config.getFile().getLogPattern());
        fileLogEncoder.start();

        RollingFileAppender logFileAppender = new RollingFileAppender();
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

        Logger log = logCtx.getLogger(pipelineName);
        log.setAdditive(false);
        log.setLevel(Level.INFO);
        log.addAppender(logConsoleAppender);
        log.addAppender(logFileAppender);

        return log;
    }
}