package org.voltdb.aggdemo;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import picocli.CommandLine;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@CommandLine.Command(name = "MediationDAtaGenerator", mixinStandardHelpOptions = true,
        description = "Usage: MediationDataGenerator hostnames userCount tpMs durationSeconds missingRatio dupRatio " +
                "lateRatio dateis1970Ratio offset where missingRatio, dupRatio, lateRatio and dateis1970Ratio are '1 in' ratios - i.e. 100 means 1%")
public class DataGeneratorConfig {

    @CommandLine.Parameters(index = "0", description = "Comma separated list of servers to connect to")
    private String hostnames;

    @CommandLine.Parameters(index = "1", description = "Number of simulated users - defines how diverse the data set is")
    private int userCount;

    @CommandLine.Parameters(index = "2", description = "Number of transactions per millisecond to generate")
    private int tpMs;

    @CommandLine.Parameters(index = "3", description = "Test duration in seconds")
    private int durationSeconds;

    @CommandLine.Parameters(index = "4", description = "Ratio of sessions with missing data")
    private int missingRatio;

    @CommandLine.Parameters(index = "5", description = "Ratio of sessions with duplicate data")
    private int dupRatio;

    @CommandLine.Parameters(index = "6", description = "Ratio of sessions with late data")
    private int lateRatio;

    @CommandLine.Parameters(index = "7", description = "Ratio of records with invalid data")
    private int dateis1970Ratio;

    @CommandLine.Parameters(index = "8", description = "Offset ;-)")
    private int offset;

    @CommandLine.Option(names = "--kafka", negatable = true, defaultValue = "true", fallbackValue = "true",
            description = "Use kafka (default). Otherwise connect directly to VoltDB")
    private boolean useKafka;

    public String getHostnames() {
        return hostnames;
    }

    public int getUserCount() {
        return userCount;
    }

    public int getTransactionsPerSecond() {
        return (int) (tpMs * TimeUnit.SECONDS.toMillis(1));
    }

    public int getDurationSeconds() {
        return durationSeconds;
    }

    public boolean shouldGenerateMissingRecord() {
        return missingRatio > 0 && ThreadLocalRandom.current().nextInt(missingRatio) == 0;
    }

    public boolean shouldGenerateDuplicatedRecord() {
        return dupRatio > 0 && ThreadLocalRandom.current().nextInt(dupRatio) == 0;
    }

    public boolean shouldGenerateLateRecord() {
        return lateRatio > 0 && ThreadLocalRandom.current().nextInt(lateRatio) == 0;
    }

    public boolean shouldGenerateWrongDateRecord() {
        return dateis1970Ratio > 0 && ThreadLocalRandom.current().nextInt(dateis1970Ratio) == 0;
    }

    public int getRandomCallingNumber() {
        return ThreadLocalRandom.current().nextInt(userCount) + offset;
    }

    public int getOffset() {
        return offset;
    }

    public boolean isUseKafka() {
        return useKafka;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
