DROP PROCEDURE CSMRT_OWNER.PS_GRP_FEE_WAIVER_P
/

--
-- PS_GRP_FEE_WAIVER_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.PS_GRP_FEE_WAIVER_P AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_GRP_FEE_WAIVER from PeopleSoft table PS_GRP_FEE_WAIVER.
--
-- V01  SMT-xxxx 04/04/2017,    Jim Doucette
--                              Converted from PS_GRP_FEE_WAIVER.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_GRP_FEE_WAIVER';
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
 where TABLE_NAME = 'PS_GRP_FEE_WAIVER'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_GRP_FEE_WAIVER@SASOURCE S)
 where TABLE_NAME = 'PS_GRP_FEE_WAIVER'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_GRP_FEE_WAIVER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_GRP_FEE_WAIVER';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_GRP_FEE_WAIVER T 
using (select /*+ full(S) */
    nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT, 
    nvl(trim(SEL_GROUP),'-') SEL_GROUP, 
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(trim(FEE_CODE),'-') FEE_CODE, 
    nvl(trim(WAIVER_CODE),'-') WAIVER_CODE, 
    nvl(SSF_WVR_PRIORITY,0) SSF_WVR_PRIORITY
from SYSADM.PS_GRP_FEE_WAIVER@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRP_FEE_WAIVER') ) S 
 on ( 
    T.BUSINESS_UNIT = S.BUSINESS_UNIT and 
    T.SEL_GROUP = S.SEL_GROUP and 
    T.EFFDT = S.EFFDT and 
    T.FEE_CODE = S.FEE_CODE and 
    T.WAIVER_CODE = S.WAIVER_CODE and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.SSF_WVR_PRIORITY = S.SSF_WVR_PRIORITY,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.SSF_WVR_PRIORITY <> S.SSF_WVR_PRIORITY or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.BUSINESS_UNIT,
    T.SEL_GROUP,
    T.EFFDT,
    T.FEE_CODE, 
    T.WAIVER_CODE,
    T.SRC_SYS_ID, 
    T.SSF_WVR_PRIORITY, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.BUSINESS_UNIT,
    S.SEL_GROUP,
    S.EFFDT,
    S.FEE_CODE, 
    S.WAIVER_CODE,
    'CS90', 
    S.SSF_WVR_PRIORITY, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_GRP_FEE_WAIVER rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_GRP_FEE_WAIVER',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_GRP_FEE_WAIVER';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_GRP_FEE_WAIVER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_GRP_FEE_WAIVER';
update CSSTG_OWNER.PS_GRP_FEE_WAIVER T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT, 
        nvl(trim(SEL_GROUP),'-') SEL_GROUP, 
        to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL 
                        else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
        nvl(trim(FEE_CODE),'-') FEE_CODE, 
        nvl(trim(WAIVER_CODE),'-') WAIVER_CODE
   from CSSTG_OWNER.PS_GRP_FEE_WAIVER T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRP_FEE_WAIVER') = 'Y'
  minus
 select nvl(trim(BUSINESS_UNIT),'-') BUSINESS_UNIT, 
        nvl(trim(SEL_GROUP),'-') SEL_GROUP, 
        to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL 
                        else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
        nvl(trim(FEE_CODE),'-') FEE_CODE, 
        nvl(trim(WAIVER_CODE),'-') WAIVER_CODE
   from SYSADM.PS_GRP_FEE_WAIVER@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_GRP_FEE_WAIVER') = 'Y'
   ) S
 where T.BUSINESS_UNIT = S.BUSINESS_UNIT
   and T.SEL_GROUP = S.SEL_GROUP
   and T.EFFDT = S.EFFDT
   and T.FEE_CODE = S.FEE_CODE
   and T.WAIVER_CODE = S.WAIVER_CODE
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_GRP_FEE_WAIVER rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_GRP_FEE_WAIVER',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_GRP_FEE_WAIVER'
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

END PS_GRP_FEE_WAIVER_P;
/
