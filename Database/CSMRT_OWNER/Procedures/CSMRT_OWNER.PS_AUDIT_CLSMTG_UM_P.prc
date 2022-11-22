DROP PROCEDURE CSMRT_OWNER.PS_AUDIT_CLSMTG_UM_P
/

--
-- PS_AUDIT_CLSMTG_UM_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_AUDIT_CLSMTG_UM_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_AUDIT_CLSMTG_UM'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_AUDIT_CLSMTG_UM', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_AUDIT_CLSMTG_UM'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_AUDIT_CLSMTG_UM from PeopleSoft table PS_AUDIT_CLSMTG_UM.
--
-- V01  SMT-xxxx 08/22/2017,    Jim Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_AUDIT_CLSMTG_UM';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN
strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_AUDIT_CLSMTG_UM'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncate',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_AUDIT_CLSMTG_UM'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'truncate table CSSTG_OWNER.PS_AUDIT_CLSMTG_UM';
begin
execute immediate 'truncate table CSSTG_OWNER.PS_AUDIT_CLSMTG_UM';
end;


strSqlCommand := 'commit';
commit;

strMessage01    := 'Loading data into CSSTG_OWNER.PS_AUDIT_CLSMTG_UM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'Insert into CSSTG_OWNER.PS_AUDIT_CLSMTG_UM';

INSERT /*+ append */
      INTO  CSSTG_OWNER.PS_AUDIT_CLSMTG_UM
   SELECT NVL (TRIM (CRSE_ID), '-') CRSE_ID,
          NVL (CRSE_OFFER_NBR, 0) CRSE_OFFER_NBR,
          NVL (TRIM (STRM), '-') STRM,
          NVL (TRIM (SESSION_CODE), '-') SESSION_CODE,
          NVL (TRIM (CLASS_SECTION), '-') CLASS_SECTION,
          NVL (CLASS_MTG_NBR, 0) CLASS_MTG_NBR,
          'CS90' SRC_SYS_ID,
          NVL (TRIM (FACILITY_ID), '-') FACILITY_ID,
          NVL (MEETING_TIME_START, TO_DATE (TO_CHAR ('01/01/1900'), 'MM/DD/YYYY'))  MEETING_TIME_START,
          NVL (MEETING_TIME_END,TO_DATE (TO_CHAR ('01/01/1900'), 'MM/DD/YYYY'))MEETING_TIME_END,
          --to_date(to_char(case when MEETING_TIME_START < '01-JAN-1800' then NULL else MEETING_TIME_START end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') MEETING_TIME_START,
          --to_date(to_char(case when MEETING_TIME_END < '01-JAN-1800' then NULL else MEETING_TIME_END end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') MEETING_TIME_END,
          NVL (TRIM (MON), '-') MON,
          NVL (TRIM (TUES), '-') TUES,
          NVL (TRIM (WED), '-') WED,
          NVL (TRIM (THURS), '-') THURS,
          NVL (TRIM (FRI), '-') FRI,
          NVL (TRIM (SAT), '-') SAT,
          NVL (TRIM (SUN), '-') SUN,
          TO_DATE (
             TO_CHAR ( CASE WHEN START_DT < '01-JAN-1800' THEN NULL ELSE START_DT END,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') START_DT,
          TO_DATE ( TO_CHAR ( CASE WHEN END_DT < '01-JAN-1800' THEN NULL ELSE END_DT END,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') END_DT,
          NVL (CRS_TOPIC_ID, 0) CRS_TOPIC_ID,
          NVL (TRIM (DESCR), '-') DESCR,
          NVL (TRIM (STND_MTG_PAT), '-') STND_MTG_PAT,
          NVL (TRIM (PRINT_TOPIC_ON_XCR), '-') PRINT_TOPIC_ON_XCR,
          'N',
          'S',
          SYSDATE,
          SYSDATE,
          1234
     FROM (SELECT CRSE_ID,
                  CRSE_OFFER_NBR,
                  STRM,
                  SESSION_CODE,
                  CLASS_SECTION,
                  CLASS_MTG_NBR,
                  FACILITY_ID,
                  MEETING_TIME_START,
                  MEETING_TIME_END,
                  MON,
                  TUES,
                  WED,
                  THURS,
                  FRI,
                  SAT,
                  SUN,
                  START_DT,
                  END_DT,
                  CRS_TOPIC_ID,
                  DESCR,
                  STND_MTG_PAT,
                  PRINT_TOPIC_ON_XCR,
                  ROW_NUMBER ()
                  OVER (
                     PARTITION BY CRSE_ID,
                                  CRSE_OFFER_NBR,
                                  STRM,
                                  SESSION_CODE,
                                  CLASS_SECTION,
                                  CLASS_MTG_NBR
                     ORDER BY
                        CRSE_ID,
                        CRSE_OFFER_NBR,
                        STRM,
                        SESSION_CODE,
                        CLASS_SECTION,
                        CLASS_MTG_NBR,
                        AUDIT_STAMP DESC)
                     AUD_ORDER
             FROM SYSADM.PS_AUDIT_CLSMTG_UM@SASOURCE S
            WHERE AUDIT_ACTN IN ('A', 'N')
          ) AUD
    WHERE AUD_ORDER = 1;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_AUDIT_CLSMTG_UM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_AUDIT_CLSMTG_UM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Indexing CSSTG_OWNER.PS_AUDIT_CLSMTG_UM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Indexing',
       END_DT = NULL
 where TABLE_NAME = 'PS_AUDIT_CLSMTG_UM'
;

strSqlCommand := 'commit';
commit;



strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = sysdate,
       OLD_MAX_SCN = 0,
       NEW_MAX_SCN = 999999999999
 where TABLE_NAME = 'PS_AUDIT_CLSMTG_UM'
;


strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
    WHEN OTHERS THEN

        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END PS_AUDIT_CLSMTG_UM_P;
/
