DROP PROCEDURE CSMRT_OWNER.AM_PS_TRNSFR_COMP_P
/

--
-- AM_PS_TRNSFR_COMP_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_TRNSFR_COMP_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_TRNSFR_COMP from PeopleSoft table PS_TRNSFR_COMP.
--
 --V01  SMT-xxxx 10/05/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_TRNSFR_COMP';
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
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_TRNSFR_COMP'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_TRNSFR_COMP@AMSOURCE S)
 where TABLE_NAME = 'PS_TRNSFR_COMP'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_TRNSFR_COMP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_TRNSFR_COMP';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_TRNSFR_COMP T
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(TRNSFR_SRC_ID),'-') TRNSFR_SRC_ID, 
    nvl(trim(COMP_SUBJECT_AREA),'-') COMP_SUBJECT_AREA, 
    EFFDT, 
    nvl(trim(TRNSFR_EQVLNCY_CMP),'-') TRNSFR_EQVLNCY_CMP, 
    replace(nvl(trim(DESCR),'-'), '  ', ' ') DESCR, 
    nvl(trim(EXT_TERM_TYPE),'-') EXT_TERM_TYPE, 
    nvl(trim(TRNSFR_CRSE_FL),'-') TRNSFR_CRSE_FL, 
    nvl(TRNSFR_PRIORITY,0) TRNSFR_PRIORITY, 
    nvl(trim(CNTNGNT_CRDT_FL),'-') CNTNGNT_CRDT_FL, 
    nvl(INP_CRSE_CNT,0) INP_CRSE_CNT, 
    nvl(trim(UNT_TRNSFR_SRC),'-') UNT_TRNSFR_SRC, 
    nvl(trim(XS_CRSE_FL),'-') XS_CRSE_FL
from SYSADM.PS_TRNSFR_COMP@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNSFR_COMP') ) S
 on ( 
    T.INSTITUTION = S.INSTITUTION and 
    T.TRNSFR_SRC_ID = S.TRNSFR_SRC_ID and 
    T.COMP_SUBJECT_AREA = S.COMP_SUBJECT_AREA and 
    T.EFFDT = S.EFFDT and 
    T.TRNSFR_EQVLNCY_CMP = S.TRNSFR_EQVLNCY_CMP and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.DESCR = S.DESCR,
    T.EXT_TERM_TYPE = S.EXT_TERM_TYPE,
    T.TRNSFR_CRSE_FL = S.TRNSFR_CRSE_FL,
    T.TRNSFR_PRIORITY = S.TRNSFR_PRIORITY,
    T.CNTNGNT_CRDT_FL = S.CNTNGNT_CRDT_FL,
    T.INP_CRSE_CNT = S.INP_CRSE_CNT,
    T.UNT_TRNSFR_SRC = S.UNT_TRNSFR_SRC,
    T.XS_CRSE_FL = S.XS_CRSE_FL,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.DESCR <> S.DESCR or 
    T.EXT_TERM_TYPE <> S.EXT_TERM_TYPE or 
    T.TRNSFR_CRSE_FL <> S.TRNSFR_CRSE_FL or 
    T.TRNSFR_PRIORITY <> S.TRNSFR_PRIORITY or 
    T.CNTNGNT_CRDT_FL <> S.CNTNGNT_CRDT_FL or 
    T.INP_CRSE_CNT <> S.INP_CRSE_CNT or 
    T.UNT_TRNSFR_SRC <> S.UNT_TRNSFR_SRC or 
    T.XS_CRSE_FL <> S.XS_CRSE_FL or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.TRNSFR_SRC_ID,
    T.COMP_SUBJECT_AREA,
    T.EFFDT,
    T.TRNSFR_EQVLNCY_CMP, 
    T.SRC_SYS_ID, 
    T.DESCR,
    T.EXT_TERM_TYPE,
    T.TRNSFR_CRSE_FL, 
    T.TRNSFR_PRIORITY,
    T.CNTNGNT_CRDT_FL,
    T.INP_CRSE_CNT, 
    T.UNT_TRNSFR_SRC, 
    T.XS_CRSE_FL, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.INSTITUTION,
    S.TRNSFR_SRC_ID,
    S.COMP_SUBJECT_AREA,
    S.EFFDT,
    S.TRNSFR_EQVLNCY_CMP, 
    'CS90', 
    S.DESCR,
    S.EXT_TERM_TYPE,
    S.TRNSFR_CRSE_FL, 
    S.TRNSFR_PRIORITY,
    S.CNTNGNT_CRDT_FL,
    S.INP_CRSE_CNT, 
    S.UNT_TRNSFR_SRC, 
    S.XS_CRSE_FL, 
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

strMessage01    := '# of PS_TRNSFR_COMP rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNSFR_COMP',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_TRNSFR_COMP';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_TRNSFR_COMP';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_TRNSFR_COMP';
update AMSTG_OWNER.PS_TRNSFR_COMP T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, EFFDT, TRNSFR_EQVLNCY_CMP
   from AMSTG_OWNER.PS_TRNSFR_COMP T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNSFR_COMP') = 'Y'
  minus
 select INSTITUTION, TRNSFR_SRC_ID, COMP_SUBJECT_AREA, EFFDT, TRNSFR_EQVLNCY_CMP
   from SYSADM.PS_TRNSFR_COMP@AMSOURCE
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_TRNSFR_COMP') = 'Y' 
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.TRNSFR_SRC_ID = S.TRNSFR_SRC_ID
   and T.COMP_SUBJECT_AREA = S.COMP_SUBJECT_AREA
   and T.EFFDT = S.EFFDT
   and T.TRNSFR_EQVLNCY_CMP = S.TRNSFR_EQVLNCY_CMP
   and T.SRC_SYS_ID = 'CS90'    
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_TRNSFR_COMP rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_TRNSFR_COMP',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_TRNSFR_COMP'
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

END AM_PS_TRNSFR_COMP_P;
/
