DROP PROCEDURE CSMRT_OWNER.PS_HEALTH_TEST_P
/

--
-- PS_HEALTH_TEST_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.PS_HEALTH_TEST_P AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_HEALTH_TEST from PeopleSoft table PS_HEALTH_TEST.
--
-- V01  SMT-xxxx 9/14/2017,    James Doucette
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_HEALTH_TEST';
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
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_HEALTH_TEST'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_HEALTH_TEST@SASOURCE S)
 where TABLE_NAME = 'PS_HEALTH_TEST'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_HEALTH_TEST';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_HEALTH_TEST'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into CSSTG_OWNER.PS_T_HEALTH_TEST
select /*+ full(S) */
       EMPLID, 
       HEALTH_TEST, 
       HEALTH_SEQ, 
       DATE_TAKEN,
       DATE_RECEIVED, 
       TEST_VALUE, 
       TEST_RESULT, 
       to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS,
       to_number(ORA_ROWSCN) SRC_SCN
  from SYSADM.PS_HEALTH_TEST@SASOURCE S
 where EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_HEALTH_TEST'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_HEALTH_TEST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_HEALTH_TEST';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_HEALTH_TEST T
    using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(HEALTH_TEST),'-') HEALTH_TEST, 
    nvl(HEALTH_SEQ,0) HEALTH_SEQ, 
    NVL(DATE_TAKEN, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) DATE_TAKEN,
    NVL(DATE_RECEIVED, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) DATE_RECEIVED, 
    nvl(TEST_VALUE,0) TEST_VALUE, 
--    nvl(trim(TEST_RESULT),'-') TEST_RESULT, 
    TEST_RESULT,
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS
from CSSTG_OWNER.PS_T_HEALTH_TEST S
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_HEALTH_TEST') ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.HEALTH_TEST = S.HEALTH_TEST and 
    T.HEALTH_SEQ = S.HEALTH_SEQ and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.DATE_TAKEN = S.DATE_TAKEN,
    T.DATE_RECEIVED = S.DATE_RECEIVED,
    T.TEST_VALUE = S.TEST_VALUE,
    T.TEST_RESULT = S.TEST_RESULT,
    T.COMMENTS = S.COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    nvl(trim(T.DATE_TAKEN),0) <> nvl(trim(S.DATE_TAKEN),0) or 
    nvl(trim(T.DATE_RECEIVED),0) <> nvl(trim(S.DATE_RECEIVED),0) or 
    T.TEST_VALUE <> S.TEST_VALUE or 
    T.TEST_RESULT <> S.TEST_RESULT or 
    nvl(trim(T.COMMENTS),0) <> nvl(trim(S.COMMENTS),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.HEALTH_TEST,
    T.HEALTH_SEQ, 
    T.SRC_SYS_ID, 
    T.DATE_TAKEN, 
    T.DATE_RECEIVED,
    T.TEST_VALUE, 
    T.TEST_RESULT,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMMENTS
    ) 
values (
    S.EMPLID, 
    S.HEALTH_TEST,
    S.HEALTH_SEQ, 
    'CS90', 
    S.DATE_TAKEN, 
    S.DATE_RECEIVED,
    S.TEST_VALUE, 
    S.TEST_RESULT,
    'N',
    'S',
    sysdate,
    sysdate,
    1234,
    S.COMMENTS)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_HEALTH_TEST rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_HEALTH_TEST',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_HEALTH_TEST';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_HEALTH_TEST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_HEALTH_TEST';
update CSSTG_OWNER.PS_HEALTH_TEST T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, HEALTH_TEST, HEALTH_SEQ
   from CSSTG_OWNER.PS_HEALTH_TEST T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_HEALTH_TEST') = 'Y'
  minus
 select EMPLID, HEALTH_TEST, HEALTH_SEQ
   from SYSADM.PS_HEALTH_TEST@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_HEALTH_TEST') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.HEALTH_TEST = S.HEALTH_TEST
   and T.HEALTH_SEQ = S.HEALTH_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_HEALTH_TEST rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_HEALTH_TEST',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_HEALTH_TEST'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


EXCEPTION
   WHEN OTHERS
   THEN
      COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION (
         i_SqlCommand   => strSqlCommand,
         i_SqlCode      => SQLCODE,
         i_SqlErrm      => SQLERRM);

END PS_HEALTH_TEST_P;
/
