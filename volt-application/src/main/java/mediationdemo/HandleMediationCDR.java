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
package mediationdemo;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Take an incoming Kafka message and aggregate it.
 */
public class HandleMediationCDR extends AbstractMediationProcedure {

    public static final SQLStmt getSession = new SQLStmt(
            "SELECT * FROM cdr_dupcheck WHERE sessionId = ? AND sessionStartUTC = ?;");

    public static final SQLStmt getSessionRunningTotals = new SQLStmt(
            "SELECT * FROM unaggregated_cdrs_by_session WHERE sessionId = ? AND sessionStartUTC = ?;");

    public static final SQLStmt updateSessionSeqnosAndUsage = new SQLStmt(
            "UPDATE cdr_dupcheck SET used_seqno_array = ?, "
                    + "unaggregated_usage = unaggregated_usage + ? WHERE sessionId = ? AND sessionStartUTC = ?;");

    public static final SQLStmt createSession = new SQLStmt(
            "INSERT INTO cdr_dupcheck "
                    + "(sessionId  , sessionStartUTC  , callingNumber , used_seqno_array "
                    + ", unaggregated_usage, insert_date)"
                    + " VALUES "
                    + "(?,?,?,?,?, NOW)");

    public static final SQLStmt createUnaggedRecordSession = new SQLStmt(
            "INSERT INTO unaggregated_cdrs  " +
                    "( sessionId, " +
                    " sessionStartUTC, " +
                    " seqno, " +
                    " callingNumber, " +
                    " destination, " +
                    " recordType, " +
                    " recordStartUTC, " +
                    " recordUsage)  " +
                    "VALUES " +
                    "(?,?,?,?,?,?,?,?); ");

    public static final SQLStmt reportBadRecord = new SQLStmt(
            "INSERT INTO bad_cdrs  " +
                    "( reason, sessionId, " +
                    " sessionStartUTC, " +
                    " seqno, " +
                    " callingNumber, " +
                    " destination, " +
                    " recordType, " +
                    " recordStartUTC, " +
                    " recordUsage)  " +
                    "VALUES " +
                    "(?,?,?,?,?,?,?,?,?); ");

    protected static final long ONE_WEEK_IN_MILLIS = TimeUnit.DAYS.toMillis(7);

    protected long aggSeqnoThreshold = 50;
    protected long aggUsageThreshold = 1000000;

    public VoltTable[] run(long sessionId, long sessionStartUTC, int seqno, String callingNumber, String destination,
                           String recordType, long recordStartUTC, long recordUsage) throws VoltAbortException {
        // We refuse to process anything that shows up more than 1 week late...
        final Date cutoffDate = new Date(this.getTransactionTime().getTime() - ONE_WEEK_IN_MILLIS);
        final Date sessionStartUTCAsDate = new Date(sessionStartUTC);
        final Date recordStartUTCAsDate = new Date(recordStartUTC);

        if (sessionStartUTCAsDate.before(cutoffDate)) {
            // This is dated more than 1 week ago - reject...
            voltQueueSQL(reportBadRecord, "LATESESSION", sessionId, sessionStartUTC, seqno, callingNumber, destination,
                    recordType, recordStartUTCAsDate, recordUsage);
            return getEmptyVoltTables();
        }

        if (recordStartUTCAsDate.before(cutoffDate)) {
            // This is dated more than 1 week ago - reject...
            voltQueueSQL(reportBadRecord, "LATERECORD", sessionId, sessionStartUTC, seqno, callingNumber, destination,
                    recordType, recordStartUTCAsDate, recordUsage);
            return getEmptyVoltTables();
        }

        // See if we know about this session, and find out what our
        // parameters are...
        voltQueueSQL(getSession, sessionId, sessionStartUTCAsDate);
        voltQueueSQL(getParameter, AGG_USAGE);
        voltQueueSQL(getParameter, AGG_SEQNOCOUNT);

        VoltTable[] sessionRecords = voltExecuteSQL();

        VoltTable sessionDupCheck = sessionRecords[0];
        aggUsageThreshold = getParameterIfSet(sessionRecords[1], aggUsageThreshold);
        aggSeqnoThreshold = getParameterIfSet(sessionRecords[2], aggSeqnoThreshold);

        // We use this to store all the sequence numbers we've seen for this session
        // instead of storing one row per sequence nuumber....
        MediationRecordSequence msr = new MediationRecordSequence(null);

        // Do duplicate checking...
        if (sessionDupCheck.advanceRow()) {

            // We've seen at least one row for this session...

            // Unload dup check record so we can see what it is
            msr = new MediationRecordSequence(sessionDupCheck.getVarbinary(3));
            boolean ourSeqIsSet = msr.getSeqno(seqno);

            if (ourSeqIsSet) {
                // This is a dup - reject...
                voltQueueSQL(reportBadRecord, "DUP", sessionId, sessionStartUTC, seqno, callingNumber, destination,
                        recordType, recordStartUTCAsDate, recordUsage);
                return getEmptyVoltTables();
            }

            // Note we've see this seqno
            msr.setSeqno(seqno);
            voltQueueSQL(updateSessionSeqnosAndUsage, msr.getSequence(), recordUsage, sessionId, sessionStartUTCAsDate);

        } else {

            // New session we've never heard of..
            msr.setSeqno(seqno);
            voltQueueSQL(createSession, sessionId, sessionStartUTCAsDate, callingNumber, msr.getSequence(), recordUsage);

        }

        // Add message contents to running totals, and then see whether we can
        // aggregate...
        voltQueueSQL(createUnaggedRecordSession, sessionId, sessionStartUTCAsDate, seqno, callingNumber, destination,
                recordType, recordStartUTCAsDate, recordUsage);
        voltQueueSQL(getSessionRunningTotals, sessionId, sessionStartUTCAsDate);

        VoltTable[] totalRecords = voltExecuteSQL();
        VoltTable totalRecordsTable = totalRecords[totalRecords.length - 1];

        aggregateSessionIfNeeded(totalRecordsTable, msr, recordType, seqno);
        return getEmptyVoltTables();
    }

    protected void aggregateSessionIfNeeded(VoltTable totalRecordsTable, MediationRecordSequence msr, String recordType,
                                            int seqno) {
        totalRecordsTable.advanceRow();

        long unaggedRecordCount = totalRecordsTable.getLong(9);
        long unaggedRecordUsage = totalRecordsTable.getLong(6);
        long sessionId = totalRecordsTable.getLong(0);
        TimestampType sessionStartUTC = totalRecordsTable.getTimestampAsTimestamp(1);

        // See if we have a complete set of records...
        if (msr.weHaveFromZeroTo(seqno)) {

            String aggReason = null;

            // Decide whether to aggregate this session
            if (recordType.equalsIgnoreCase("E")) {
                aggReason = "END";
            } else if (unaggedRecordCount > aggSeqnoThreshold) {
                aggReason = "QTY";
            } else if (unaggedRecordUsage > aggUsageThreshold) {
                aggReason = "USAGE";
            }

            if (aggReason != null) {

                aggregateSession(totalRecordsTable, aggReason);

                deleteSessionRunningTotals(sessionId, sessionStartUTC);

                voltExecuteSQL();
            }

        }
    }

    private VoltTable[] getEmptyVoltTables() {
        voltExecuteSQL();
        return new VoltTable[0];
    }
}
