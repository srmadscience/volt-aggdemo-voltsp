DROP TASK FlushStaleSessionsTask IF EXISTS;

DROP PROCEDURE GetBySessionId IF EXISTS; 

DROP PROCEDURE HandleMediationCDR IF EXISTS; 
   
DROP PROCEDURE FlushStaleSessions IF EXISTS;

DROP PROCEDURE ShowAggStatus__promBL IF EXISTS;

DROP VIEW total_unaggregated_usage IF EXISTS;

DROP VIEW cdr_dupcheck_agg_summary_minute IF EXISTS;

DROP VIEW cdr_dupcheck_session_summary_minute IF EXISTS;

DROP VIEW unaggregated_cdrs_by_session IF EXISTS;

DROP TABLE mediation_parameters IF EXISTS;

DROP TABLE cdr_dupcheck IF EXISTS;

DROP STREAM bad_cdrs IF EXISTS;

DROP STREAM aggregated_cdrs IF EXISTS;

DROP STREAM unaggregated_cdrs IF EXISTS;

DROP FUNCTION getHighestValidSequence IF EXISTS

DROP FUNCTION sequenceToString FROM METHOD IF EXISTS;
