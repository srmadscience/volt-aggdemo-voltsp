package org.voltdb.aggdemo;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import com.google_voltpatches.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.voltdb.aggdemo.connection.Connection;
import org.voltdb.aggdemo.connection.ConnectionFactory;
import picocli.CommandLine;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * This generates mock CDRS that need to be aggregated. It also deliberately
 * introduces the same kind of mistakes we see in the real world, such as
 * duplicate records, missing records and late records.
 */
public class MediationDataGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(MediationDataGenerator.class);

    private final DataGeneratorConfig config;
    private final Connection sender;

    int normalCDRCount;
    int missingCount;
    int dupCount;
    int lateCount;
    int dateis1970Count;

    HashMap<String, MediationSession> sessionMap = new HashMap<>();
    ArrayList<MediationMessage> dupMessages = new ArrayList<>();
    ArrayList<MediationMessage> lateMessages = new ArrayList<>();

    public MediationDataGenerator(DataGeneratorConfig config) {
        this.config = config;

        ConnectionFactory connectionFactory = ConnectionFactory.create(config);
        sender = connectionFactory.create(config.getHostnames());

        LOGGER.info(config.toString());
        run();
    }

    public void run() {
        long startTime = System.currentTimeMillis();
        long endTime = startTime + TimeUnit.SECONDS.toMillis(config.getDurationSeconds());

        long lastStatsTime = System.currentTimeMillis();

        long sessionId = 0;
        long recordCount = 0;
        long lastReportedRecordCount = 0;

        RateLimiter rateLimiter = RateLimiter.create(config.getTransactionsPerSecond());
        while (System.currentTimeMillis() < endTime) {
            rateLimiter.acquire();

            recordCount++;
            String randomCallingNumber = "Num" + config.getRandomCallingNumber();

            MediationSession ourSession = sessionMap.get(randomCallingNumber);
            if (ourSession == null) {
                ourSession = new MediationSession(
                        randomCallingNumber,
                        getRandomDestinationId(),
                        config.getRandomCallingNumber() + sessionId++
                );

                sessionMap.put(randomCallingNumber, ourSession);
            }

            mainSendingPart(ourSession.getNextCdr());
            if (shouldFlushLateAndDupMessages()) {
                sendRemainingMessages();
            }

            if (shouldPrintStatus(lastStatsTime)) {
                double recordsProcessed = recordCount - lastReportedRecordCount;
                double tps = 1000 * (recordsProcessed / (System.currentTimeMillis() - lastStatsTime));

                LOGGER.info("Offset = " + config.getOffset() + " Record " + recordCount + " TPS=" + (long) tps);
                LOGGER.info("Active Sessions: " + sessionMap.size());

                lastStatsTime = System.currentTimeMillis();
                lastReportedRecordCount = recordCount;
                //	printApplicationStats(voltClient,nextCdr);
            }
        }

        sendRemainingMessages();
        printGeneralStatus();
    }

    private void mainSendingPart(MediationMessage nextCdr) {
        // Now decide what to do. We could just send the CDR, but where's the fun in
        // that?
        if (config.shouldGenerateMissingRecord()) {
            // Let's just pretend this CDR never happened...
            missingCount++;
        } else if (config.shouldGenerateDuplicatedRecord()) {

            // let's send it. Lots of times...
            for (int i = 0; i < 2 + ThreadLocalRandom.current().nextInt(10); i++) {
                sender.sendData(nextCdr);
            }

            // Also add it to a list of dup messages to send again, later...
            dupMessages.add(nextCdr);
            dupCount++;

        } else if (config.shouldGenerateLateRecord()) {
            // Add it to a list of late messages to send later...
            lateMessages.add(nextCdr);
            lateCount++;

        } else if (config.shouldGenerateWrongDateRecord()) {
            // Set date to Jan 1, 1970, and then send it...
            nextCdr.setRecordStartUTC(0);
            sender.sendData(nextCdr);
            dateis1970Count++;
        } else {
            sender.sendData(nextCdr);
            normalCDRCount++;
        }
    }

    private static boolean shouldPrintStatus(long laststatstime) {
        return laststatstime + 10000 < System.currentTimeMillis();
    }

    private boolean shouldFlushLateAndDupMessages() {
        return lateMessages.size() > 100000 || dupMessages.size() > 100000;
    }

    /**
     * Print general status info
     */
    private void printGeneralStatus() {
        LOGGER.info("normalCDRCount = " + normalCDRCount);
        LOGGER.info("missingCount = " + missingCount);
        LOGGER.info("dupCount = " + dupCount);
        LOGGER.info("lateCount = " + lateCount);
        LOGGER.info("dateis1970Count = " + dateis1970Count);
    }

    /**
     * Send any messages in the late or duplicates queues. Note this is not rate
     * limited, and may cause a latency spike
     */
    private void sendRemainingMessages() {
        // Send late messages
        LOGGER.info("sending " + lateMessages.size() + " late messages");

        while (!lateMessages.isEmpty()) {
            MediationMessage lateCDR = lateMessages.remove(0);
            sender.sendData(lateCDR);
        }

        // Send dup messages
        LOGGER.info("sending " + dupMessages.size() + " duplicate messages");
        while (!dupMessages.isEmpty()) {
            MediationMessage dupCDR = dupMessages.remove(0);
            sender.sendData(dupCDR);
        }
    }

    /**
     * @return A random website.
     */
    private String getRandomDestinationId() {
        if (ThreadLocalRandom.current().nextInt(10) == 0) {
            return "www.nytimes.com";
        }

        if (ThreadLocalRandom.current().nextInt(10) == 0) {
            return "www.cnn.com";
        }

        return "www.voltdb.com";
    }

    public static void main(String[] args) {
        DataGeneratorConfig generatorConfig = new DataGeneratorConfig();
        CommandLine commandLine = new CommandLine(generatorConfig);

        CommandLine.ParseResult parseResult = commandLine.parseArgs(args);
        if (parseResult.isUsageHelpRequested()) {
            commandLine.usage(System.out);
        } else if (parseResult.isVersionHelpRequested()) {
            commandLine.printVersionHelp(System.out);
        } else {
            new MediationDataGenerator(generatorConfig);
        }
    }
}
