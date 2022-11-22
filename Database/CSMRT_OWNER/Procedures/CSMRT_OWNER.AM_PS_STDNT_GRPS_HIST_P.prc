DROP PROCEDURE CSMRT_OWNER.AM_PS_STDNT_GRPS_HIST_P
/

--
-- AM_PS_STDNT_GRPS_HIST_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER.AM_PS_STDNT_GRPS_HIST_P IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_STDNT_GRPS_HIST from PeopleSoft table PS_STDNT_GRPS_HIST.
--
-- V01  SMT-xxxx 05/11/2017,    Jim Doucette
--                              Converted from PS_STDNT_GRPS_HIST.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_STDNT_GRPS_HIST';
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
 where TABLE_NAME = 'PS_STDNT_GRPS_HIST'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_STDNT_GRPS_HIST@AMSOURCE S)
 where TABLE_NAME = 'PS_STDNT_GRPS_HIST'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table AMSTG_OWNER.PS_T_STDNT_GRPS_HIST';
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
 where TABLE_NAME = 'PS_STDNT_GRPS_HIST'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
insert /*+ append */  into AMSTG_OWNER.PS_T_STDNT_GRPS_HIST
select /*+ full(S) */
nvl(trim(EMPLID),'-') EMPLID,
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(STDNT_GROUP),'-') STDNT_GROUP,
nvl(EFFDT, to_date('01-JAN-1900')) EFFDT,
'CS90' SRC_SYS_ID,
nvl(trim(EFF_STATUS),'-') EFF_STATUS,
substr(to_char(trim(COMMENTS)),1,4000) COMMENTS,
to_number(ORA_ROWSCN) SRC_SCN
  from SYSADM.PS_STDNT_GRPS_HIST@AMSOURCE S
 where EMPLID between '00000000' and '99999999'
   and length(EMPLID) = 8
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_STDNT_GRPS_HIST'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_STDNT_GRPS_HIST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_STDNT_GRPS_HIST';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_STDNT_GRPS_HIST T
using (select /*+ full(S) */
    nvl(trim(EMPLID),'-') EMPLID,
    nvl(trim(INSTITUTION),'-') INSTITUTION,
    nvl(trim(STDNT_GROUP),'-') STDNT_GROUP,
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
    nvl(trim(EFF_STATUS),'-') EFF_STATUS,
    COMMENTS COMMENTS
from AMSTG_OWNER.PS_T_STDNT_GRPS_HIST S
where SRC_SCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_GRPS_HIST') ) S
   on (
    T.EMPLID = S.EMPLID and
    T.INSTITUTION = S.INSTITUTION and
    T.STDNT_GROUP = S.STDNT_GROUP and
    T.EFFDT = S.EFFDT and
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.COMMENTS = S.COMMENTS,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID   = 1234
where
    T.EFF_STATUS <> S.EFF_STATUS or
    nvl(trim(T.COMMENTS),0) <> nvl(trim(S.COMMENTS),0) or
    T.DATA_ORIGIN = 'D'
when not matched then
insert (
    T.EMPLID,
    T.INSTITUTION,
    T.STDNT_GROUP,
    T.EFFDT,
    T.SRC_SYS_ID,
    T.EFF_STATUS,
    T.LOAD_ERROR,
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID,
    T.COMMENTS
    )
values (
    S.EMPLID,
    S.INSTITUTION,
    S.STDNT_GROUP,
    S.EFFDT,
    'CS90',
    S.EFF_STATUS,
    'N',
    'S',
    sysdate,
    sysdate,
    1234,
    S.COMMENTS);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_STDNT_GRPS_HIST rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_GRPS_HIST',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_STDNT_GRPS_HIST';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_STDNT_GRPS_HIST';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_STDNT_GRPS_HIST';
update AMSTG_OWNER.PS_STDNT_GRPS_HIST T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select EMPLID, INSTITUTION, STDNT_GROUP, EFFDT
   from AMSTG_OWNER.PS_STDNT_GRPS_HIST T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_GRPS_HIST') = 'Y'
  minus
 select EMPLID, INSTITUTION, STDNT_GROUP, EFFDT
   from SYSADM.PS_STDNT_GRPS_HIST@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_STDNT_GRPS_HIST') = 'Y'
   ) S
 where T.EMPLID = S.EMPLID 
   and T.INSTITUTION = S.INSTITUTION
   and T.STDNT_GROUP = S.STDNT_GROUP
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90') 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_STDNT_GRPS_HIST rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_STDNT_GRPS_HIST',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_STDNT_GRPS_HIST'
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

END AM_PS_STDNT_GRPS_HIST_P;
/
