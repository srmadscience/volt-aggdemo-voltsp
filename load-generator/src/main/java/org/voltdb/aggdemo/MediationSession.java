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
package org.voltdb.aggdemo;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Class representing a user session in our mediation demo.
 */
public class MediationSession {

    public static final String SESSION_START = "S";
    public static final String SESSION_INTERMEDIATE = "I";
    public static final String SESSION_END = "E";
    public static final int MAX_POSSIBLE_SEQNO = 255;

    private long sessionStartUTC;
    private String callingNumber;
    private String destination;
    private long sessionid;
    private int seqno;
    private int maxSeqno;

    /**
     * Create a simulated device that will produce different kinds of CDRS...
     *
     * @param callingNumber - Our device ID
     * @param destination   - A website the device is speaking to
     * @param sessionid     - A Unique ID
     */
    public MediationSession(String callingNumber, String destination, long sessionid) {
        this.destination = destination;
        this.sessionid = sessionid;
        this.callingNumber = callingNumber;

        this.seqno = 0;
        this.sessionStartUTC = System.currentTimeMillis();
        this.maxSeqno = ThreadLocalRandom.current().nextInt(MAX_POSSIBLE_SEQNO);
    }

    /**
     * Calling this method advances the state of this session. A session state
     * begins with SESSION_START, then has an arbitrary number of
     * SESSION_INTERMEDIATE's and finally a SESSION_END. Each time we are called we
     * increment seqno. Just to be difficult the returned CDR only has the
     * callingNumber when it's a SESSION_START
     *
     * @return A new, unique, CDR
     */
    public MediationMessage getNextCdr() {
        MediationMessage newCDR = new MediationMessage(sessionid, sessionStartUTC, seqno, null, destination);
        newCDR.setRecordStartUTC(System.currentTimeMillis());

        switch (seqno) {
            case 0:
                // Note that this is the *only* time we identify the phone
                newCDR.setCallingNumber(callingNumber);
                newCDR.setEventType(SESSION_START);
                newCDR.setRecordUsage(ThreadLocalRandom.current().nextInt(100000));
                seqno++;
                break;

            case MAX_POSSIBLE_SEQNO:
                newCDR.setEventType(SESSION_END);
                newCDR.setRecordUsage(ThreadLocalRandom.current().nextInt(100));
                sessionStartUTC = System.currentTimeMillis();
                seqno = 0;
                break;

            default:
                newCDR.setEventType(SESSION_INTERMEDIATE);
                newCDR.setRecordUsage(ThreadLocalRandom.current().nextInt(100000));
                seqno++;
        }

        return newCDR;
    }
}
