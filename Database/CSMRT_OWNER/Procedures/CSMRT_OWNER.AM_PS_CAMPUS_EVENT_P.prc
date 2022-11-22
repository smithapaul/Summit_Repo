DROP PROCEDURE CSMRT_OWNER.AM_PS_CAMPUS_EVENT_P
/

--
-- AM_PS_CAMPUS_EVENT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_CAMPUS_EVENT_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_CAMPUS_EVENT from PeopleSoft table PS_CAMPUS_EVENT.
--
-- V01  SMT-xxxx 8/31/2017,    James Doucette
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_CAMPUS_EVENT';
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

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = SYSDATE,
       END_DT = NULL
 where TABLE_NAME = 'PS_CAMPUS_EVENT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_CAMPUS_EVENT@AMSOURCE S)
 where TABLE_NAME = 'PS_CAMPUS_EVENT'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_CAMPUS_EVENT';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strSqlCommand   := 'Loading temp table for AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Loading'
 where TABLE_NAME = 'PS_CAMPUS_EVENT'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
INSERT /*+ append */
  INTO AMSTG_OWNER.PS_T_CAMPUS_EVENT
SELECT /*+ full(S) */
       nvl(trim(CAMPUS_EVENT_NBR),'-') CAMPUS_EVENT_NBR,
       nvl(trim(INSTITUTION),'-') INSTITUTION, 
       nvl(trim(DESCR),'-') DESCR, 
       nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
       nvl(trim(CAMPUS_EVENT_TYPE),'-') CAMPUS_EVENT_TYPE, 
       nvl(trim(EVENT_MANAGER),'-') EVENT_MANAGER, 
       nvl(PRIMARY_MEETING,0) PRIMARY_MEETING, 
       nvl(ATTENDEE_NBR_LST,0) ATTENDEE_NBR_LST,
       to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS,
       to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_CAMPUS_EVENT@AMSOURCE S;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_CAMPUS_EVENT'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_CAMPUS_EVENT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_CAMPUS_EVENT';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_CAMPUS_EVENT T 
using (select /*+ full(S) */
    nvl(trim(CAMPUS_EVENT_NBR),'-') CAMPUS_EVENT_NBR, 
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(CAMPUS_EVENT_TYPE),'-') CAMPUS_EVENT_TYPE, 
    nvl(trim(EVENT_MANAGER),'-') EVENT_MANAGER, 
    nvl(PRIMARY_MEETING,0) PRIMARY_MEETING, 
    nvl(ATTENDEE_NBR_LST,0) ATTENDEE_NBR_LST, 
    to_char(substr(trim(COMMENTS), 1, 4000)) COMMENTS
from AMSTG_OWNER.PS_T_CAMPUS_EVENT S
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CAMPUS_EVENT') ) S 
 on ( 
    T.CAMPUS_EVENT_NBR = S.CAMPUS_EVENT_NBR and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.INSTITUTION = S.INSTITUTION,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.CAMPUS_EVENT_TYPE = S.CAMPUS_EVENT_TYPE,
    T.EVENT_MANAGER = S.EVENT_MANAGER,
    T.PRIMARY_MEETING = S.PRIMARY_MEETING,
    T.ATTENDEE_NBR_LST = S.ATTENDEE_NBR_LST,
    T.COMMENTS = S.COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.INSTITUTION <> S.INSTITUTION or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.CAMPUS_EVENT_TYPE <> S.CAMPUS_EVENT_TYPE or 
    T.EVENT_MANAGER <> S.EVENT_MANAGER or 
    T.PRIMARY_MEETING <> S.PRIMARY_MEETING or 
    T.ATTENDEE_NBR_LST <> S.ATTENDEE_NBR_LST or 
    nvl(trim(T.COMMENTS),0) <> nvl(trim(S.COMMENTS),0) or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.CAMPUS_EVENT_NBR, 
    T.SRC_SYS_ID, 
    T.INSTITUTION,
    T.DESCR,
    T.DESCRSHORT, 
    T.CAMPUS_EVENT_TYPE,
    T.EVENT_MANAGER,
    T.PRIMARY_MEETING,
    T.ATTENDEE_NBR_LST, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMMENTS
    ) 
values (
    S.CAMPUS_EVENT_NBR, 
    'CS90', 
    S.INSTITUTION,
    S.DESCR,
    S.DESCRSHORT, 
    S.CAMPUS_EVENT_TYPE,
    S.EVENT_MANAGER,
    S.PRIMARY_MEETING,
    S.ATTENDEE_NBR_LST,  
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


strMessage01    := '# of PS_CAMPUS_EVENT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CAMPUS_EVENT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_CAMPUS_EVENT';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_CAMPUS_EVENT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_CAMPUS_EVENT';
update AMSTG_OWNER.PS_CAMPUS_EVENT T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select CAMPUS_EVENT_NBR
   from AMSTG_OWNER.PS_CAMPUS_EVENT T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CAMPUS_EVENT') = 'Y'
  minus
 select CAMPUS_EVENT_NBR
   from SYSADM.PS_CAMPUS_EVENT@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_CAMPUS_EVENT') = 'Y'
   ) S
 where T.CAMPUS_EVENT_NBR = S.CAMPUS_EVENT_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_CAMPUS_EVENT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_CAMPUS_EVENT',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_CAMPUS_EVENT'
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

END AM_PS_CAMPUS_EVENT_P;
/
