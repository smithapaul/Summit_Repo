CREATE OR REPLACE PROCEDURE             "PS_SAA_ADB_RESULTS_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_SAA_ADB_RESULTS'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_SAA_ADB_RESULTS', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_SAA_ADB_RESULTS'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_SAA_ADB_RESULTS from PeopleSoft table PS_SAA_ADB_RESULTS.
--
-- V01  SMT-xxxx 08/01/2017,    Jim Doucette
--                              Converted from PS_SAA_ADB_RESULTS.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_SAA_ADB_RESULTS';
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
 where TABLE_NAME = 'PS_SAA_ADB_RESULTS'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Disable',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_SAA_ADB_RESULTS'
;

strSqlCommand := 'commit';
commit;

strSqlCommand  := 'alter table CSSTG_OWNER.PS_SAA_ADB_RESULTS disable constraint PK_PS_SAA_ADB_RESULTS';
begin
execute immediate 'alter table CSSTG_OWNER.PS_SAA_ADB_RESULTS disable constraint PK_PS_SAA_ADB_RESULTS';
end;


strSqlCommand := 'commit';
commit;


strSqlCommand   := 'truncate table CSSTG_OWNER.PS_SAA_ADB_RESULTS';
begin
execute immediate 'truncate table CSSTG_OWNER.PS_SAA_ADB_RESULTS';
end;


strSqlCommand := 'commit';
commit;



strMessage01    := 'Loading data into CSSTG_OWNER.PS_SAA_ADB_RESULTS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'Insert into CSSTG_OWNER.PS_SAA_ADB_RESULTS';
insert /*+ append */  into CSSTG_OWNER.PS_SAA_ADB_RESULTS
select /*+ full(R) */ 
       EMPLID, 
       ANALYSIS_DB_SEQ, 
       SAA_CAREER_RPT, 
       SAA_ENTRY_SEQ, 
       'CS90' SRC_SYS_ID, 
       RPT_DATE, 
       trim(TSCRPT_TYPE), 
       trim(SAA_RPT_IDENTIFIER), 
       trim(ENTRY_R_TYPE), 
       trim(ACAD_CAREER), 
       trim(ACAD_PROG), 
       trim(ACAD_PLAN), 
       trim(ACAD_SUB_PLAN), 
       RQ_DATE, 
       trim(RQRMNT_GROUP), 
       trim(REQUIREMENT), 
       trim(RQ_LINE_NBR), 
       trim(RQRMNT_LIST_SEQ), 
       trim(REQ_LINE_TYPE), 
       trim(DISP_SELECT_LINE), 
       trim(PARTITION_FAIL), 
       UNITS_REQUIRED, 
       SAA_UNITS_USED, 
       UNITS_NEEDED, 
       CRSES_REQUIRED, 
       SAA_CRSES_USED, 
       CRSES_NEEDED, 
       SAA_CRSE_COUNT, 
       GPA_REQUIRED, 
       GPA_ACTUAL, 
       RQ_AA_OVERRIDE, 
       ASOF_DATE, 
       RQ_OVRD_ACTION, 
       OLD_MIN_UNITS, 
       NEW_MIN_UNITS, 
       OLD_MAX_UNITS, 
       NEW_MAX_UNITS, 
       OLD_MIN_CRSES, 
       NEW_MIN_CRSES, 
       OLD_MAX_CRSES, 
       NEW_MAX_CRSES, 
       trim(REF_RQMT_GROUP), 
       trim(REF_REQUIREMENT), 
       trim(ITEM_R_STATUS), 
       trim(SAA_PRINT_CNTL), 
       trim(SAA_DISPLAY_OPTION), 
       trim(SAA_DISPLAY_GPA), 
       trim(SAA_DISPLAY_UNITS), 
       trim(SAA_DISPLAY_CRSCNT), 
       trim(OPRID), 
       trim(SAA_OPRID_NAME), 
       trim(DESCR254A), 
       trim(SAA_HIDE_STATUS), 
       sysdate CREATED_EW_DTTM
  from SYSADM.PS_SAA_ADB_RESULTS@SASOURCE R 
 where TSCRPT_TYPE in ('DADV','LADV','DGPA','LGPA')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SAA_ADB_RESULTS rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SAA_ADB_RESULTS',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Indexing CSSTG_OWNER.PS_SAA_ADB_RESULTS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Indexing',
       END_DT = NULL
 where TABLE_NAME = 'PS_SAA_ADB_RESULTS'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Enable contraint CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'enable constraint PK_PS_SAA_ADB_RESULTS';
strSqlCommand  := 'alter table CSSTG_OWNER.PS_SAA_ADB_RESULTS enable constraint PK_PS_SAA_ADB_RESULTS';
begin
execute immediate 'alter table CSSTG_OWNER.PS_SAA_ADB_RESULTS enable constraint PK_PS_SAA_ADB_RESULTS';
end;


strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = sysdate,
       OLD_MAX_SCN = 0,
       NEW_MAX_SCN = 999999999999
 where TABLE_NAME = 'PS_SAA_ADB_RESULTS'
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

END PS_SAA_ADB_RESULTS_P;
/
