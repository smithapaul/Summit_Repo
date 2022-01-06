CREATE OR REPLACE PROCEDURE             "PS_ENRL_MSG_LOG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ENRL_MSG_LOG from PeopleSoft table PS_ENRL_MSG_LOG.
--
-- V01  SMT-xxxx 05/11/2017,    Jim Doucette
--                              Converted from PS_ENRL_MSG_LOG.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ENRL_MSG_LOG';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        strDELETE_FLG                   Varchar2(1);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        intOLD_MAX_SCN                  Integer         := 0;
        intNEW_MAX_SCN                  Integer         := 0;
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
 where TABLE_NAME = 'PS_ENRL_MSG_LOG'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ENRL_MSG_LOG@SASOURCE S)
 where TABLE_NAME = 'PS_ENRL_MSG_LOG'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Selecting variables from CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

select DELETE_FLG,
       OLD_MAX_SCN,
       NEW_MAX_SCN
  into strDELETE_FLG,
       intOLD_MAX_SCN,
       intNEW_MAX_SCN
  from CSSTG_OWNER.UM_STAGE_JOBS
 where TABLE_NAME = 'PS_ENRL_MSG_LOG'
;

strMessage01    := 'Merging data into CSSTG_OWNER.PS_ENRL_MSG_LOG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ENRL_MSG_LOG';
merge /*+ use_hash(S,T) parallel(8) enable_parallel_dml */ into CSSTG_OWNER.PS_ENRL_MSG_LOG T
using (select /*+ full(S) */
    nvl(trim(ENRL_REQUEST_ID),'-') ENRL_REQUEST_ID,
    nvl(ENRL_REQ_DETL_SEQ,0) ENRL_REQ_DETL_SEQ,
    nvl(MESSAGE_SEQ,0) MESSAGE_SEQ,
    nvl(MESSAGE_SET_NBR,0) MESSAGE_SET_NBR,
    nvl(MESSAGE_NBR,0) MESSAGE_NBR,
    nvl(trim(MSG_SEVERITY),'-') MSG_SEVERITY,
    to_date(to_char(case when DTTM_STAMP_SEC < '01-JAN-1800' then NULL
                    else DTTM_STAMP_SEC end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') DTTM_STAMP_SEC
  from SYSADM.PS_ENRL_MSG_LOG@SASOURCE S
-- where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ENRL_MSG_LOG') ) S
 where ORA_ROWSCN > intOLD_MAX_SCN) S
 on (
    T.ENRL_REQUEST_ID = S.ENRL_REQUEST_ID and
    T.ENRL_REQ_DETL_SEQ = S.ENRL_REQ_DETL_SEQ and
    T.MESSAGE_SEQ = S.MESSAGE_SEQ and
    T.SRC_SYS_ID = 'CS90')
    when matched then update set
    T.MESSAGE_SET_NBR = S.MESSAGE_SET_NBR,
    T.MESSAGE_NBR = S.MESSAGE_NBR,
    T.MSG_SEVERITY = S.MSG_SEVERITY,
    T.DTTM_STAMP_SEC = S.DTTM_STAMP_SEC,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where
    T.MESSAGE_SET_NBR <> S.MESSAGE_SET_NBR or
    T.MESSAGE_NBR <> S.MESSAGE_NBR or
    T.MSG_SEVERITY <> S.MSG_SEVERITY or
    nvl(trim(T.DTTM_STAMP_SEC),0) <> nvl(trim(S.DTTM_STAMP_SEC),0) or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.ENRL_REQUEST_ID,
    T.ENRL_REQ_DETL_SEQ,
    T.MESSAGE_SEQ,
    T.SRC_SYS_ID,
    T.MESSAGE_SET_NBR,
    T.MESSAGE_NBR,
    T.MSG_SEVERITY,
    T.DTTM_STAMP_SEC,
    T.LOAD_ERROR,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
)
values (
    S.ENRL_REQUEST_ID,
    S.ENRL_REQ_DETL_SEQ,
    S.MESSAGE_SEQ,
    'CS90',
    S.MESSAGE_SET_NBR,
    S.MESSAGE_NBR,
    S.MSG_SEVERITY,
    S.DTTM_STAMP_SEC,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ENRL_MSG_LOG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ENRL_MSG_LOG',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

If strDELETE_FLG = 'Y' then

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ENRL_MSG_LOG';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ENRL_MSG_LOG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ENRL_MSG_LOG';
update /*+ parallel(8) enable_parallel_dml */ CSSTG_OWNER.PS_ENRL_MSG_LOG T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists
(select 1 from
(select ENRL_REQUEST_ID, ENRL_REQ_DETL_SEQ, MESSAGE_SEQ
   from CSSTG_OWNER.PS_ENRL_MSG_LOG T2
--  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ENRL_MSG_LOG') = 'Y'
  minus
 select ENRL_REQUEST_ID, ENRL_REQ_DETL_SEQ, MESSAGE_SEQ
   from SYSADM.PS_ENRL_MSG_LOG@SASOURCE S2
--  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ENRL_MSG_LOG') = 'Y'
   ) S
 where T.ENRL_REQUEST_ID = S.ENRL_REQUEST_ID
   and T.ENRL_REQ_DETL_SEQ = S.ENRL_REQ_DETL_SEQ
   and T.MESSAGE_SEQ = S.MESSAGE_SEQ
   and T.SRC_SYS_ID = 'CS90'
   )
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ENRL_MSG_LOG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ENRL_MSG_LOG',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

End if;

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ENRL_MSG_LOG'
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

END PS_ENRL_MSG_LOG_P;
/
