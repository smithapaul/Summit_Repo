CREATE OR REPLACE PROCEDURE             PS_VISA_PERMIT_TBL_P AUTHID CURRENT_USER IS


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_VISA_PERMIT_TBL from PeopleSoft table PS_VISA_PERMIT_TBL.
--
-- V01  SMT-xxxx 9/26/2017,    James Doucette
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_VISA_PERMIT_TBL';
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
 where TABLE_NAME = 'PS_VISA_PERMIT_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_VISA_PERMIT_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_VISA_PERMIT_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_VISA_PERMIT_TBL';
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
 where TABLE_NAME = 'PS_VISA_PERMIT_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into CSSTG_OWNER.PS_T_VISA_PERMIT_TBL
select /*+ full(S) */
    nvl(trim(COUNTRY),'-') COUNTRY, 
    nvl(trim(VISA_PERMIT_TYPE),'-') VISA_PERMIT_TYPE, 
    EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(VISA_PERMIT_CLASS),'-') VISA_PERMIT_CLASS, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_VISA_PERMIT_TBL@SASOURCE S; 

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_VISA_PERMIT_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_VISA_PERMIT_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_VISA_PERMIT_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_VISA_PERMIT_TBL T
    using (select /*+ full(S) */
    nvl(trim(COUNTRY),'-') COUNTRY, 
    nvl(trim(VISA_PERMIT_TYPE),'-') VISA_PERMIT_TYPE, 
    EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(VISA_PERMIT_CLASS),'-') VISA_PERMIT_CLASS, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    COMMENTS,
    to_number(ORA_ROWSCN) SRC_SCN
from CSSTG_OWNER.PS_T_VISA_PERMIT_TBL S 
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_VISA_PERMIT_TBL') ) S
 on ( 
    T.COUNTRY = S.COUNTRY and 
    T.VISA_PERMIT_TYPE = S.VISA_PERMIT_TYPE and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.VISA_PERMIT_CLASS = S.VISA_PERMIT_CLASS,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.COMMENTS = S.COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.VISA_PERMIT_CLASS <> S.VISA_PERMIT_CLASS or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    nvl(trim(T.COMMENTS),0) <> nvl(trim(S.COMMENTS),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.COUNTRY,
    T.VISA_PERMIT_TYPE, 
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.VISA_PERMIT_CLASS,
    T.DESCR,
    T.DESCRSHORT, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMMENTS
    ) 
values (
    S.COUNTRY,
    S.VISA_PERMIT_TYPE, 
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.VISA_PERMIT_CLASS,
    S.DESCR,
    S.DESCRSHORT, 
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


strMessage01    := '# of PS_VISA_PERMIT_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_VISA_PERMIT_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_VISA_PERMIT_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_VISA_PERMIT_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_VISA_PERMIT_TBL';
update CSSTG_OWNER.PS_VISA_PERMIT_TBL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select COUNTRY, VISA_PERMIT_TYPE, EFFDT
   from CSSTG_OWNER.PS_VISA_PERMIT_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_VISA_PERMIT_TBL') = 'Y'
  minus
 select COUNTRY, VISA_PERMIT_TYPE, EFFDT
   from SYSADM.PS_VISA_PERMIT_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_VISA_PERMIT_TBL') = 'Y'
   ) S
 where T.COUNTRY = S.COUNTRY
   and T.VISA_PERMIT_TYPE = S.VISA_PERMIT_TYPE
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_VISA_PERMIT_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_VISA_PERMIT_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_VISA_PERMIT_TBL'
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

END PS_VISA_PERMIT_TBL_P;
/
