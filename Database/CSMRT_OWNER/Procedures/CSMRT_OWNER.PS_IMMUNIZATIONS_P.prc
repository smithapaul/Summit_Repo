CREATE OR REPLACE PROCEDURE             PS_IMMUNIZATIONS_P AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_IMMUNIZATIONS from PeopleSoft table PS_IMMUNIZATIONS.
--
-- V01  SMT-xxxx 9/15/2017,    James Doucette
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_IMMUNIZATIONS';
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
 where TABLE_NAME = 'PS_IMMUNIZATIONS'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_IMMUNIZATIONS@SASOURCE S)
 where TABLE_NAME = 'PS_IMMUNIZATIONS'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_IMMUNIZATIONS';
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
 where TABLE_NAME = 'PS_IMMUNIZATIONS'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into CSSTG_OWNER.PS_T_IMMUNIZATIONS
select /*+ full(S) */
    EMPLID, 
    IMMUNIZATION, 
    IMMUN_SEQ, 
    DATE_TAKEN,
    EXPIRATION_DT, 
    DATE_RECEIVED, 
    STATUS_IMMUN, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_IMMUNIZATIONS@SASOURCE S
where EMPLID between '00000000' and '99999999'
  and length(EMPLID) = 8 
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_IMMUNIZATIONS'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_IMMUNIZATIONS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_IMMUNIZATIONS';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_IMMUNIZATIONS T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID, 
    nvl(trim(IMMUNIZATION),'-') IMMUNIZATION, 
    nvl(IMMUN_SEQ,0) IMMUN_SEQ, 
    DATE_TAKEN,
    EXPIRATION_DT, 
    DATE_RECEIVED, 
    nvl(trim(STATUS_IMMUN),'-') STATUS_IMMUN, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS
from CSSTG_OWNER.PS_T_IMMUNIZATIONS S 
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_IMMUNIZATIONS') ) S
 on ( 
    T.EMPLID = S.EMPLID and 
    T.IMMUNIZATION = S.IMMUNIZATION and 
    T.IMMUN_SEQ = S.IMMUN_SEQ and 
    T.SRC_SYS_ID = 'CS90') 
when matched then update set
    T.DATE_TAKEN = S.DATE_TAKEN,
    T.EXPIRATION_DT = S.EXPIRATION_DT,
    T.DATE_RECEIVED = S.DATE_RECEIVED,
    T.STATUS_IMMUN = S.STATUS_IMMUN,
    T.COMMENTS = S.COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    nvl(trim(T.DATE_TAKEN),0) <> nvl(trim(S.DATE_TAKEN),0) or 
    nvl(trim(T.EXPIRATION_DT),0) <> nvl(trim(S.EXPIRATION_DT),0) or 
    nvl(trim(T.DATE_RECEIVED),0) <> nvl(trim(S.DATE_RECEIVED),0) or 
    T.STATUS_IMMUN <> S.STATUS_IMMUN or 
    nvl(trim(T.COMMENTS),0) <> nvl(trim(S.COMMENTS),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.EMPLID, 
    T.IMMUNIZATION, 
    T.IMMUN_SEQ,
    T.SRC_SYS_ID, 
    T.DATE_TAKEN, 
    T.EXPIRATION_DT,
    T.DATE_RECEIVED,
    T.STATUS_IMMUN, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMMENTS
    ) 
values (
    S.EMPLID, 
    S.IMMUNIZATION, 
    S.IMMUN_SEQ,
    'CS90', 
    S.DATE_TAKEN, 
    S.EXPIRATION_DT,
    S.DATE_RECEIVED,
    S.STATUS_IMMUN,  
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


strMessage01    := '# of PS_IMMUNIZATIONS rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_IMMUNIZATIONS',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_IMMUNIZATIONS';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_IMMUNIZATIONS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_IMMUNIZATIONS';
update CSSTG_OWNER.PS_IMMUNIZATIONS T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, IMMUNIZATION, IMMUN_SEQ
   from CSSTG_OWNER.PS_IMMUNIZATIONS T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_IMMUNIZATIONS') = 'Y'
  minus
 select EMPLID, IMMUNIZATION, IMMUN_SEQ
   from SYSADM.PS_IMMUNIZATIONS@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_IMMUNIZATIONS') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID
   and T.IMMUNIZATION = S.IMMUNIZATION
   and T.IMMUN_SEQ = S.IMMUN_SEQ
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_IMMUNIZATIONS rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_IMMUNIZATIONS',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_IMMUNIZATIONS'
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

END PS_IMMUNIZATIONS_P;
/
