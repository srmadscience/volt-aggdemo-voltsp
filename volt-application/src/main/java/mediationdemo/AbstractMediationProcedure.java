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
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/**
 * Abstract procedure class that contains shared aggregation and cancelation functionality.
 */
public abstract class AbstractMediationProcedure extends VoltProcedure {

    public static final SQLStmt getParameter = new SQLStmt(
            "SELECT parameter_value FROM mediation_parameters WHERE parameter_name = ? ;");

    public static final SQLStmt deleteSessionRunningTotals = new SQLStmt(
            "DELETE FROM unaggregated_cdrs_by_session WHERE sessionId = ? AND sessionStartUTC = ?;");

    public static final SQLStmt createAggregatedSession = new SQLStmt(
            "INSERT INTO aggregated_cdrs  " +
                    "( reason, sessionId, " +
                    " sessionStartUTC, " +
                    " min_seqno, max_seqno, " +
                    " callingNumber, " +
                    " destination, " +
                    " startAggTimeUTC, endAggTimeUTC," +
                    " recordUsage)  " +
                    "VALUES " +
                    "(?,?,?, ?,?,?, ?,?,?, ?); ");

    public static final SQLStmt reportBadRange = new SQLStmt(
            "INSERT INTO bad_cdrs  " +
                    "( reason, sessionId, " +
                    " sessionStartUTC, " +
                    " seqno,end_seqno, " +
                    " callingNumber, " +
                    " destination, " +
                    " recordType, " +
                    " recordStartUTC, end_recordStartUTC, " +
                    " recordUsage)  " +
                    "VALUES " +
                    "(?,?,?,?,?,?,?,?,?,?,?); ");

    public static final SQLStmt updateAggStatus = new SQLStmt(
            "UPDATE cdr_dupcheck SET last_agg_date = NOW, agg_state = ?"
                    + ", aggregated_usage = aggregated_usage + ?"
                    + ", unaggregated_usage = 0 "
                    + "WHERE sessionId = ? AND sessionStartUTC = ?;");

    protected static final String AGG_USAGE = "AGG_USAGE";
    protected static final String AGG_SEQNOCOUNT = "AGG_SEQNOCOUNT";
    protected static final String STALENESS_THRESHOLD_MS = "STALENESS_THRESHOLD_MS";
    protected static final String AGG_WINDOW_SIZE_MS = "AGG_WINDOW_SIZE_MS";
    protected static final Object STALENESS_ROWLIMIT = "STALENESS_ROWLIMIT";

    /**
     * Aggregate a session. We assume that totalRecordsTable is currently on the right row.
     */
    protected void aggregateSession(VoltTable totalRecordsTable, String aggReason) {
        // Unload data from record
        long minSeqno = totalRecordsTable.getLong(4);
        long maxSeqno = totalRecordsTable.getLong(5);
        TimestampType startDate = totalRecordsTable.getTimestampAsTimestamp(2);
        TimestampType endDate = totalRecordsTable.getTimestampAsTimestamp(3);
        long sessionId = totalRecordsTable.getLong(0);
        TimestampType sessionStartUTC = totalRecordsTable.getTimestampAsTimestamp(1);
        String callingNumber = totalRecordsTable.getString(7);
        String destination = totalRecordsTable.getString(8);
        long unaggedRecordUsageToReport = totalRecordsTable.getLong(6);

        //Create an aggregated session
        voltQueueSQL(createAggregatedSession, aggReason, sessionId, sessionStartUTC, minSeqno, maxSeqno, callingNumber,
                destination, startDate, endDate, unaggedRecordUsageToReport);

        // Report change in status
        voltQueueSQL(updateAggStatus, aggReason, unaggedRecordUsageToReport, sessionId, sessionStartUTC);

        // Delete unneeded records
        deleteSessionRunningTotals(sessionId, sessionStartUTC);
    }

    /**
     * Cancel a late session. We assume that totalRecordsTable is currently on the right row.
     */
    protected void cancelLateSession(VoltTable sessionToClose) {
        // Unload data from record
        long sessionId = sessionToClose.getLong(0);
        TimestampType sessionStartUTC = sessionToClose.getTimestampAsTimestamp(1);
        long minSeqno = sessionToClose.getLong(4);
        long maxSeqno = sessionToClose.getLong(5);
        TimestampType startDate = sessionToClose.getTimestampAsTimestamp(2);
        TimestampType endDate = sessionToClose.getTimestampAsTimestamp(3);
        String callingNumber = sessionToClose.getString(7);
        String destination = sessionToClose.getString(8);
        long unaggedRecordUsageToReport = sessionToClose.getLong(6);

        // Cancel session
        voltQueueSQL(reportBadRange, "LATE", sessionId, sessionStartUTC, minSeqno, maxSeqno, callingNumber, destination,
                "RANGE", startDate, endDate, unaggedRecordUsageToReport);

        // Report change in status
        voltQueueSQL(updateAggStatus, "LATE", 0, sessionId, sessionStartUTC);

        // Delete unneeded records
        deleteSessionRunningTotals(sessionId, sessionStartUTC);
    }

    /**
     * Delete a row from the stream view unaggregated_cdrs_by_session. Being a stream
     * view this doesn't happen by itself.
     */
    protected void deleteSessionRunningTotals(long sessionId, TimestampType sessionStartUTC) {
        voltQueueSQL(deleteSessionRunningTotals, sessionId, sessionStartUTC);
    }

    /**
     * Get parameter at current row in voltTable, or return defaultValue
     * if not found.
     */
    protected long getParameterIfSet(VoltTable voltTable, long defaultValue) {
        if (voltTable.advanceRow()) {
            return (voltTable.getLong("parameter_value"));
        }

        return defaultValue;
    }
}
