CREATE OR REPLACE PROCEDURE             "PS_AUDIT_APPROG_UM_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_AUDIT_APPROG_UM from PeopleSoft table PS_AUDIT_APPROG_UM.
--
-- V01  SMT-xxxx 10/05/2017,    Jim Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_AUDIT_APPROG_UM';
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
 where TABLE_NAME = 'PS_AUDIT_APPROG_UM'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update START_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncate',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_AUDIT_APPROG_UM'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'truncate table CSSTG_OWNER.PS_AUDIT_APPROG_UM';
begin
execute immediate 'truncate table CSSTG_OWNER.PS_AUDIT_APPROG_UM';
end;


strSqlCommand := 'commit';
commit;

strMessage01    := 'Loading data into CSSTG_OWNER.PS_AUDIT_APPROG_UM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'Insert into CSSTG_OWNER.PS_AUDIT_APPROG_UM';
insert /*+ append */  into CSSTG_OWNER.PS_AUDIT_APPROG_UM
select /*+ full(S) */
    AUDIT_OPRID, 
    TO_CHAR(AUDIT_STAMP, 'YYYY-MM-DD HH24:MI:SSxFF') AUDIT_STAMP,
    AUDIT_ACTN, 
    EMPLID, 
    ACAD_CAREER, 
    STDNT_CAR_NBR, 
    ADM_APPL_NBR, 
    APPL_PROG_NBR, 
    EFFDT, 
    EFFSEQ, 
    INSTITUTION, 
    ACAD_PROG, 
    PROG_STATUS, 
    PROG_ACTION, 
    ACTION_DT, 
    PROG_REASON, 
    ADMIT_TERM, 
    EXP_GRAD_TERM, 
    REQ_TERM, 
    ACAD_LOAD_APPR,
    'CS90',
    'N',
    'S',
    sysdate,
    sysdate,
    1234
 from SYSADM.PS_AUDIT_APPROG_UM@SASOURCE R 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_AUDIT_APPROG_UM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_AUDIT_APPROG_UM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Indexing CSSTG_OWNER.PS_AUDIT_APPROG_UM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Indexing',
       END_DT = NULL
 where TABLE_NAME = 'PS_AUDIT_APPROG_UM'
;

strSqlCommand := 'commit';
commit;



strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = sysdate,
       OLD_MAX_SCN = 0,
       NEW_MAX_SCN = 999999999999
 where TABLE_NAME = 'PS_AUDIT_APPROG_UM'
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

END PS_AUDIT_APPROG_UM_P;
/
