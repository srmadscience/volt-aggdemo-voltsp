
# voltdb-aggdemo-voltsp
A demonstration of how VoltDB can be used for the kind of streaming aggregation tasks common in Telco. This demo has been modified to use the VoltSP functionality inrtroduced in V14.0.

# Overview
“Mediation” is the process of taking a firehouse of records generated by devices on a network and turning
it into a stream of validated and well organized records that are useful to downstream systems. Mediation
can also involve “Correlation”, which is joining two or more related streams of data that don’t have a 1:1
relationship with each other.
While legacy Mediation systems tended to revolve around the need to produce paper bills from telephone
switching equipment, the fundamental requirement to turn a chaotic stream of events into a manageable
stream of valid records is now relevant to a whole series of areas such as video game analytics, the IoT, and
more or less any scenario where events are being reported by devices ‘in the wild.
In theory processing this data sounds very simple and could easily be done with a simple RDBMS or even a
streaming SQL product.

But:

* THE VOLUMES ARE HUGE

It’s perfectly normal to see billions of records an hour, or more then 280K / second. A system also needs to
be able to process records at at least 1.5 times the speed they are created, so it can catch up after an outage.
The rules for turning the incoming stream into an outgoing stream are complicated.
In theory you can do mediation with a SQL ‘GROUP BY’ operation. In practice the rules for when to send a
record downstream are highly complicated and domain specific.

*  LATE RECORDS

A small proportion of records which show up will be either out of sequence or simply late. When possible
they need to be processed. This means that some sessions will last much longer than others.


* DUPLICATE RECORDS CAN’T BE TOLERATED

If we don’t spot duplicates, we run the risk of double counting usage and overcharging end users. This
sounds like a simple issue, but we’re expecting billions of records per day and need to keep them for days.
Note that overwriting a record with one that has the same key but different values could lead to values
changing ‘after the fact’ - once we’ve aggregated a record we can’t go back and do it again.

* MISSING RECORDS

In the real world, you will inevitably have some records that never make it to their destination. Your system
needs to cope with these rationally.

* INCORRECTLY TIMESTAMPED RECORDS

In real world systems we sometimes see records that are from devices that either have wildly inaccurate
clocks or have never set their system clock. Your system needs to prevent these from being processed.

* WE SOMETIMES NEED TO INSPECT ‘IN FLIGHT’ DATA

In many emerging Use Cases the data stream is looked at in two ways. While most aggregate data
influences events over a time period of hours or days, a subset of the data deals refers to ongoing events
that can be influenced over timescales ranging from milliseconds to seconds. This is challenging, as
traditionally we can do high volumes in minutes, or low volumes in milliseconds, but not high volumes
in milliseconds.

# WHAT THE MEDIATION APPLICATION DEMONSTRATES

Mediation has turned into a complex, multi-faceted problem. The issues we have to address are not just
relevant to Telco, but also to Video Game Analytics, stock trade reports and other streaming data sets.

* STATELESS AND STAEFUL PROCESSING (NEW)

In the original version of the demo we send traffic directly into Volt from a load generator. In this version we've added VoltSP, our stateless processing component. VoltSP has two tasks in this version:

1. Refuse to process records with creation dates more than a week in the past.
2. Convert the JSON format of the record into CSV, which Volt prefers.

* LOW LATENCY, HIGH VOLUME TRANSACTIONS

The mediation application can process hundreds of thousands of records per second, while still allowing
users to inspect individual sessions in real time.

* COMPLEX DECISIONS

Each mediation decision is a non-trivial event, and involves both sanity checking the incoming record as
well as making an individual, context aware decision on whether to generate an output record.

* TRANSACTIONAL CONSISTENCY

The mediation application uses Volt Active Data’s architecture to provide immediately consistent answers at
mass scale, without re-calculation afterwards or giving misleading answers due to ‘eventual consistency’.
We can guarantee that the numbers being sent downstream accurately reflect the data arriving.

* HIGH AVAILABILITY

The system will continue to run if a node is brought down. The node will rejoin without problems.

* SCALE

We will show that we can support 10’s of thousands of transactions per second on a 3 node generic cluster in AWS.

* CLOUD NATIVE

This sandbox is created using an AWS CloudFormation script. We could have done the whole thing in Kubernetes,
but that means that anyone using the sandbox needs to know Kubernetes. For simplicity, we’ve left it out.

* VISUALIZED RESULTS

The sandbox includes a Grafana dashboard that allows you see what’s going on from an Operating System,
Database and Business perspective.

# MEDIATION

This sandbox shows how Volt Active Data can be used to aggregate high volume streaming events. There
are numerous situations in Telco, the IoT, and other areas where we need to do this at scale.
This example is based on the author’s experience writing code to handle billions of records in the Telco
industry. While it’s simplistic, the challenges it deals with are all real ones that have been seen in the field.
In this case we are receiving a firehose of records and turning them into a quality, consolidated feed.
Each record describes internet usage for a subscriber’s session as they use the web.

The record structure looks like this:


|FIELD|PURPOSE|EXAMPLE|
|---|---|---|
|SessionId| Unique Id from equipment generating data. Resets when device is restarted after an outage.| 456|
|sessionStartUTC|Start time of session in UTC. SessionId + session StartUTC identifies a session. Adding Seqno makes it unique | 2-Feb-21 03:46:34 GMT |
|callingNumber |The user who is doing the work |555-1212|
|Seqno|An ascending gap free integer between 0 and 255|37|
|recordType|There will be one ‘S’ (Start), more than one ‘I’ (intermediate) and one ‘E’ (end).| S|
|recordStartUTC| Generation time of record in UTC. |2-Feb-21 03:46:34 GMT|
|recordUsage| Bytes if usage during this period 50600| 5|

So a session with 5 records will look like this:

|SessionID|SessionStartUTC|callingNumber|Seqno|recordType|recordStartUTC|recordUsage
|---|---|---|---|---|---|---|
|456|2-Feb-21 03:46:34 GMT|555-1212|0|S|2-Feb-21 03:46:34 GMT|400|
||||1 |I |2-Feb-21 03:56:34 GMT |327|
||||2 |I |2-Feb-21 04:16:34 GMT |0|
||||3 |I |2-Feb-21 05:16:34 GMT |800|
||||4 |E |2-Feb-21 05:17:34 GMT |1100|

When a session’s seqno gets to 255 it will be ended and will then start again. As a consequence, a session
can run more or less forever.

Unfortunately, we live in an imperfect world and problems occur. Although they are rare, we are processing
billions of records a day, so 1 in a billion events happen every couple of hours.

# Our Scenario’s Aggregation requirements:
* We need to be able to support 50,000 records per second.
* We process all records we receive that are up to 7 days old.
* Under no circumstances do we process a duplicate record.
* Under no circumstances do we process a complete session with a missing record. Such sessions are reported.
* Records or sessions that we hear about that are more than 7 days old are rejected.
* Event based Aggregation: We cut output records when the session ends or when we’ve seen more than 1,000,000 bytes of usage.
* Time based Aggregation: We cut output records when we haven’t seen traffic for a while.
* Duplicates and other malformed records we receive are output to a separate queue.

# HOW WE HANDLE THESE REQUIREMENTS
* Volt Active Data appears externally as a Kafka cluster.
* The text client generates CDRs and puts them into the queue. Each of these CDRs is processed by a
* Volt Active Data stored procedure on arrival.
* A scheduled task also runs every second to check for sessions that are inactive and need to be processed.
* The output consists of 2 kafka streams aggregated_cdrs and bad_cdrs.
* Users can also inspect a session status in real time by calling the stored procedure GetBySessionId.

## HIGH VOLUMES
Volt Active Data has a proven track record in asynchronously handling high volume streams of individual events.
## DUPLICATE DETECTION
In a traditional solution we would solve the dup check requirement by storing a single dup checking
record for every row we receive. Given that we have to keep records for 7 days this is wasteful of storage.
We also notice that the seqno component of the key is an ascending sequential integer between 0 and 255.
We therefore create a table called ‘cdr_dupcheck’. Where the primary key is that of the session as a whole
(SessionId + sessionStartUTC), and then add a 32 byte array of binary data, with each bit representing a
seqno. This means we can use a single record to check for uniqueness for all 256 possible seqnos in a session.
## EVENT BASED AGGREGATION
As each CDR arrives we update our running totals and decide whether we need to output a record because the
session has finished, we’ve seen too many intermediate records or because the total recorded usage needs to
be sent downstream.
## TIME BASED AGGREGATION
We have a scheduled task that runs on all of Volt Active Data’s partitions and will aggregate or error out
sessions that are inactive or broken.
## TIME BASED SANITY CHECKING
Our demo rejects records that are more than 1 week old
## HOW OUR TEST DATA GENERATOR WORKS
Our goal is to pretend to be a large number of separate device sessions, each of which is stepping its way
through the “Start-Intermediate-End” lifecycle. The code we use to do this can be seen here.
It works by creating a Java HashMap of MediationSession objects, each of which, as the name suggests,
represents a session. It then picks random session Ids and either creates a new session if one doesn’t exist, or
advances the state of the session if it does. The message is then sent to Volt Active Data via Kafka. This means
that sessions progress at different rates, and initially you won’t see any output, until a session finishes or has
some other reason for producing output.
Note that depending on how many sessions you are emulating you may run low on RAM on your test
generation server.

# Running the code

The code lives in the 'jars' directory and is called voltdb-aggdemo-client.jar

It takes the following parameters:

|Name|Meaning|Example Value|
|-|-|-|
|hostnames|Which db servers we’re connecting to. Comma delimited.|vdb1,vdb2,vdb3|
|userCount|How many users we have|500000|
|tpms|How many transactions to attempt each millisecond.|80|
|durationseconds|How many seconds to run for|1800|
|missingRatio|How often to do a global status query. Should be less than durationseconds|600|
|dupRatio|How often we produce a duplicate record. A value of ‘2000’ means that 1 in 2000 records will be a duplicate. A value of ‘-1’ disables duplicate records|2000|
|lateRatio|How often we produce a late record. Late records will be for valid sessions, but will be delivered out of sequence. A value of ‘2000’ means that 1 in 2000 records will be late. A value of ‘-1’ disables late records|2000|
|dateis1970Ratio|How often we produce a late record with an unreasonably early timestamp we can’t process. A value of ‘2000’ means that 1 in 2000 records will be for 1-Jan-1970. A value of ‘-1’ disables these records|2000|
|offset|Number to add to session id’s, which normally start at zero. Used when we want to run multiple copies of the generator at the same time.|0|

An example usage would be:

````java -jar voltdb-aggdemo-client.jar vdb1,vdb2,vdb3 100000 80 1200 -1 -1 -1 -1 0````


![Test](https://www.google-analytics.com/collect?v=1&cid=1&t=pageview&ec=repo&ea=open&dp=srmadscience%2Fvoly-aggdemo-voltsp&dt=srmadscience%2Fvoly-aggdemo-voltsp&tid=G-QZZ7G3CH8D)
