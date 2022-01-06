CREATE OR REPLACE PROCEDURE             "PS_UM_ITEM_TYPE_FA_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_UM_ITEM_TYPE_FA from PeopleSoft table PS_UM_ITEM_TYPE_FA.
--
--V01  SMT-xxxx 08/09/2017,    James Doucette
--                              Converted from PS_UM_ITEM_TYPE_FA.SQL
--
--V02  SMT-xxxx 09/01/2018,    George Adams
--                              Fixed several problems for SMT-7991
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_UM_ITEM_TYPE_FA';
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
 where TABLE_NAME = 'PS_UM_ITEM_TYPE_FA'
;

strSqlCommand := 'commit';
commit;

strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
--       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACAD_CAL_TABLE@SASOURCE S)
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_UM_ITEM_TYPE_FA@SASOURCE S)      -- Sept 2018 
 where TABLE_NAME = 'PS_UM_ITEM_TYPE_FA'
;

strSqlCommand := 'commit';
commit;

strMessage01    := 'Merging data into CSSTG_OWNER.PS_UM_ITEM_TYPE_FA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_UM_ITEM_TYPE_FA';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_UM_ITEM_TYPE_FA T
using (select /*+ full(S) */
nvl(trim(SETID),'-') SETID,
nvl(trim(ITEM_TYPE),'-') ITEM_TYPE,
nvl(trim(AID_YEAR),'-') AID_YEAR,
nvl(trim(UM_DEPT_CNTCT),'-') UM_DEPT_CNTCT,
nvl(trim(UM_DONOR_CNTCT),'-') UM_DONOR_CNTCT,
nvl(trim(UM_ENDOW_PRJ),'-') UM_ENDOW_PRJ,
nvl(trim(UM_DEPARTMENT),'-') UM_DEPARTMENT,
nvl(trim(DESCR254),'-') DESCR254
from SYSADM.PS_UM_ITEM_TYPE_FA@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_ITEM_TYPE_FA') ) S
   on (
T.SETID = S.SETID and
T.ITEM_TYPE = S.ITEM_TYPE and
T.AID_YEAR = S.AID_YEAR and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.UM_DEPT_CNTCT = S.UM_DEPT_CNTCT,
T.UM_DONOR_CNTCT = S.UM_DONOR_CNTCT,
T.UM_ENDOW_PRJ = S.UM_ENDOW_PRJ,
T.UM_DEPARTMENT = S.UM_DEPARTMENT,
T.DESCR254 = S.DESCR254,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = SYSDATE,
T.BATCH_SID   = 1234
where
T.UM_DEPT_CNTCT <> S.UM_DEPT_CNTCT or
T.UM_DONOR_CNTCT <> S.UM_DONOR_CNTCT or
T.UM_ENDOW_PRJ <> S.UM_ENDOW_PRJ or
T.UM_DEPARTMENT <> S.UM_DEPARTMENT or
T.DESCR254 <> S.DESCR254 or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.SETID,
T.ITEM_TYPE,
T.AID_YEAR,
T.SRC_SYS_ID,
T.UM_DEPT_CNTCT,
T.UM_DONOR_CNTCT,
T.UM_ENDOW_PRJ,
T.UM_DEPARTMENT,
T.DESCR254,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID
)
values (
S.SETID,
S.ITEM_TYPE,
S.AID_YEAR,
'CS90',
S.UM_DEPT_CNTCT,
S.UM_DONOR_CNTCT,
S.UM_ENDOW_PRJ,
S.UM_DEPARTMENT,
S.DESCR254,
'N',
'S',
SYSDATE,
SYSDATE,
1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_ITEM_TYPE_FA rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_ITEM_TYPE_FA',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_UM_ITEM_TYPE_FA';

strSqlCommand := 'commit';
commit;

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_UM_ITEM_TYPE_FA';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_UM_ITEM_TYPE_FA';
update CSSTG_OWNER.PS_UM_ITEM_TYPE_FA T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select SETID, ITEM_TYPE, AID_YEAR
   from CSSTG_OWNER.PS_UM_ITEM_TYPE_FA T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_ITEM_TYPE_FA') = 'Y'
  minus
 select SETID, ITEM_TYPE, AID_YEAR
   from SYSADM.PS_UM_ITEM_TYPE_FA@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_UM_ITEM_TYPE_FA') = 'Y'
   ) S
 where T.SETID = S.SETID
   and T.ITEM_TYPE = S.ITEM_TYPE
   and T.AID_YEAR = S.AID_YEAR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_UM_ITEM_TYPE_FA rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_UM_ITEM_TYPE_FA',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_UM_ITEM_TYPE_FA'
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

END PS_UM_ITEM_TYPE_FA_P;
/
