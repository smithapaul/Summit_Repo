CREATE OR REPLACE PROCEDURE             "PS_CITIZENSHIP_P" AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_CITIZENSHIP from PeopleSoft table PS_CITIZENSHIP.
--
 --V01  SMT-xxxx 09/01/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_CITIZENSHIP';
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
 where TABLE_NAME = 'PS_CITIZENSHIP'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CITIZENSHIP@SASOURCE S)
 where TABLE_NAME = 'PS_CITIZENSHIP'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_CITIZENSHIP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_CITIZENSHIP';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_CITIZENSHIP T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(DEPENDENT_ID),'-') DEPENDENT_ID, 
    nvl(trim(COUNTRY),'-') COUNTRY, 
    nvl(trim(CITIZENSHIP_STATUS),'-') CITIZENSHIP_STATUS, 
    nvl(trim(WORKER_TYPE_SGP),'-') WORKER_TYPE_SGP, 
    NVL(PERM_STATUS_DT_SGP, to_date(to_char('01/01/1900'), 'MM/DD/YYYY' )) PERM_STATUS_DT_SGP
from SYSADM.PS_CITIZENSHIP@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CITIZENSHIP') ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.DEPENDENT_ID = S.DEPENDENT_ID and 
    T.COUNTRY = S.COUNTRY and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.CITIZENSHIP_STATUS = S.CITIZENSHIP_STATUS,
    T.WORKER_TYPE_SGP = S.WORKER_TYPE_SGP,
    T.PERM_STATUS_DT_SGP = S.PERM_STATUS_DT_SGP,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.CITIZENSHIP_STATUS <> S.CITIZENSHIP_STATUS or 
    T.WORKER_TYPE_SGP <> S.WORKER_TYPE_SGP or 
    T.PERM_STATUS_DT_SGP <> S.PERM_STATUS_DT_SGP or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.DEPENDENT_ID, 
    T.COUNTRY,
    T.SRC_SYS_ID, 
    T.CITIZENSHIP_STATUS, 
    T.WORKER_TYPE_SGP,
    T.PERM_STATUS_DT_SGP, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.EMPLID, 
    S.DEPENDENT_ID, 
    S.COUNTRY,
    'CS90', 
    S.CITIZENSHIP_STATUS, 
    S.WORKER_TYPE_SGP,
    S.PERM_STATUS_DT_SGP, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234)
;


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CITIZENSHIP rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CITIZENSHIP',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CITIZENSHIP';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_CITIZENSHIP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_CITIZENSHIP';
update CSSTG_OWNER.PS_CITIZENSHIP T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(DEPENDENT_ID),'-') DEPENDENT_ID, 
    nvl(trim(COUNTRY),'-') COUNTRY
   from CSSTG_OWNER.PS_CITIZENSHIP T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CITIZENSHIP') = 'Y'
  minus
 select nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(DEPENDENT_ID),'-') DEPENDENT_ID, 
    nvl(trim(COUNTRY),'-') COUNTRY
   from SYSADM.PS_CITIZENSHIP@SASOURCE
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CITIZENSHIP') = 'Y' 
   ) S
 where T.EMPLID = S.EMPLID   
   and T.DEPENDENT_ID = S.DEPENDENT_ID
   and T.COUNTRY = S.COUNTRY
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CITIZENSHIP rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CITIZENSHIP',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CITIZENSHIP'
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

END PS_CITIZENSHIP_P;
/
