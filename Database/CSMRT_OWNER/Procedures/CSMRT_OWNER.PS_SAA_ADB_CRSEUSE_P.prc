DROP PROCEDURE CSMRT_OWNER.PS_SAA_ADB_CRSEUSE_P
/

--
-- PS_SAA_ADB_CRSEUSE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_SAA_ADB_CRSEUSE_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_SAA_ADB_CRSEUSE'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_SAA_ADB_CRSEUSE', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_SAA_ADB_CRSEUSE'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_SAA_ADB_CRSEUSE from PeopleSoft table PS_SAA_ADB_CRSEUSE.
--
-- V01  SMT-xxxx 08/01/2017,    Jim Doucette
--                              Converted from PS_SAA_ADB_CRSEUSE.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_SAA_ADB_CRSEUSE';
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
 where TABLE_NAME = 'PS_SAA_ADB_CRSEUSE'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Disable',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_SAA_ADB_CRSEUSE'
;

strSqlCommand := 'commit';
commit;

strSqlCommand  := 'alter table CSSTG_OWNER.PS_SAA_ADB_CRSEUSE disable constraint PK_PS_SAA_ADB_CRSEUSE';
begin
execute immediate 'alter table CSSTG_OWNER.PS_SAA_ADB_CRSEUSE disable constraint PK_PS_SAA_ADB_CRSEUSE';
end;


strSqlCommand := 'commit';
commit;


strSqlCommand   := 'truncate table CSSTG_OWNER.PS_SAA_ADB_CRSEUSE';
begin
execute immediate 'truncate table CSSTG_OWNER.PS_SAA_ADB_CRSEUSE';
end;


strSqlCommand := 'commit';
commit;



strMessage01    := 'Loading data into CSSTG_OWNER.PS_SAA_ADB_CRSEUSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'Insert into CSSTG_OWNER.PS_SAA_ADB_CRSEUSE';
insert /*+ append */  into CSSTG_OWNER.PS_SAA_ADB_CRSEUSE
select /*+ full(C) */
       EMPLID, 
       ANALYSIS_DB_SEQ, 
       SAA_CAREER_RPT, 
       SAA_ENTRY_SEQ, 
       SAA_COURSE_SEQ,
       CRSE_TAG, 
       'CS90' SRC_SYS_ID,
       RPT_DATE, 
       trim(SEL_PROCESS_TYPE) SEL_PROCESS_TYPE, 
       trim(SEL_MODE) SEL_MODE, 
       trim(IN_PROGRESS_GRD) IN_PROGRESS_GRD, 
       UNT_TAKEN, 
       UNT_EARNED, 
       CRSE_COUNT, 
       trim(RQ_AA_OVERRIDE) RQ_AA_OVERRIDE, 
       ASOF_DATE, 
       trim(DIRCT_TYPE) DIRCT_TYPE, 
       trim(SAA_DISPLAY_OPTION) SAA_DISPLAY_OPTION, 
       trim(OPRID) OPRID, 
       trim(DESCR254A) DESCR254A,
       sysdate CREATED_EW_DTTM
  from SYSADM.PS_SAA_ADB_CRSEUSE@SASOURCE C 
 where TSCRPT_TYPE in ('DADV','LADV','DGPA','LGPA')
   and EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_SAA_ADB_CRSEUSE rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_SAA_ADB_CRSEUSE',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Indexing CSSTG_OWNER.PS_SAA_ADB_CRSEUSE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Indexing',
       END_DT = NULL
 where TABLE_NAME = 'PS_SAA_ADB_CRSEUSE'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Enable contraint CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'enable constraint PK_PS_SAA_ADB_CRSEUSE';
strSqlCommand  := 'alter table CSSTG_OWNER.PS_SAA_ADB_CRSEUSE enable constraint PK_PS_SAA_ADB_CRSEUSE';
begin
execute immediate 'alter table CSSTG_OWNER.PS_SAA_ADB_CRSEUSE enable constraint PK_PS_SAA_ADB_CRSEUSE';
end;


strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = sysdate,
       OLD_MAX_SCN = 0,
       NEW_MAX_SCN = 999999999999
 where TABLE_NAME = 'PS_SAA_ADB_CRSEUSE'
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

END PS_SAA_ADB_CRSEUSE_P;
/
