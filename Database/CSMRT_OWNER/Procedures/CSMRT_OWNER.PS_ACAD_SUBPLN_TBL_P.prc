DROP PROCEDURE CSMRT_OWNER.PS_ACAD_SUBPLN_TBL_P
/

--
-- PS_ACAD_SUBPLN_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_ACAD_SUBPLN_TBL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ACAD_SUBPLN_TBL from PeopleSoft table PS_ACAD_SUBPLN_TBL.
--
-- V01  SMT-xxxx 04/21/2017,    Jim Doucette
--                              Converted from PS_ACAD_SUBPLN_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ACAD_SUBPLN_TBL';
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
 where TABLE_NAME = 'PS_ACAD_SUBPLN_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACAD_SUBPLN_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_ACAD_SUBPLN_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ACAD_SUBPLN_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ACAD_SUBPLN_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ACAD_SUBPLN_TBL T
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(ACAD_PLAN),'-') ACAD_PLAN, 
    nvl(trim(ACAD_SUB_PLAN),'-') ACAD_SUB_PLAN, 
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL 
                    else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(ACAD_SUBPLAN_TYPE),'-') ACAD_SUBPLAN_TYPE, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(SUBPLN_REQTRM_DFLT),'-') SUBPLN_REQTRM_DFLT, 
    nvl(trim(DIPLOMA_DESCR),'-') DIPLOMA_DESCR, 
    nvl(trim(DIPLOMA_PRINT_FL),'-') DIPLOMA_PRINT_FL, 
    nvl(DIPLOMA_INDENT,0) DIPLOMA_INDENT, 
    nvl(trim(TRNSCR_DESCR),'-') TRNSCR_DESCR, 
    nvl(trim(TRNSCR_PRINT_FL),'-') TRNSCR_PRINT_FL, 
    nvl(TRNSCR_INDENT,0) TRNSCR_INDENT, 
    nvl(trim(FIRST_TERM_VALID),'-') FIRST_TERM_VALID, 
    nvl(trim(CIP_CODE),'-') CIP_CODE, 
    nvl(trim(HEGIS_CODE),'-') HEGIS_CODE, 
    nvl(trim(TRANSCRIPT_LEVEL),'-') TRANSCRIPT_LEVEL, 
    nvl(trim(EVALUATE_SUBPLAN),'-') EVALUATE_SUBPLAN, 
    to_date(to_char(case when SSR_LAST_PRS_DT < '01-JAN-1800' then NULL 
                    else SSR_LAST_PRS_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SSR_LAST_PRS_DT, 
    nvl(trim(SSR_LAST_ADM_TERM),'-') SSR_LAST_ADM_TERM
from SYSADM.PS_ACAD_SUBPLN_TBL@SASOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_SUBPLN_TBL') ) S
on ( 
    T.INSTITUTION = S.INSTITUTION and 
    T.ACAD_PLAN = S.ACAD_PLAN and 
    T.ACAD_SUB_PLAN = S.ACAD_SUB_PLAN and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.ACAD_SUBPLAN_TYPE = S.ACAD_SUBPLAN_TYPE,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.SUBPLN_REQTRM_DFLT = S.SUBPLN_REQTRM_DFLT,
    T.DIPLOMA_DESCR = S.DIPLOMA_DESCR,
    T.DIPLOMA_PRINT_FL = S.DIPLOMA_PRINT_FL,
    T.DIPLOMA_INDENT = S.DIPLOMA_INDENT,
    T.TRNSCR_DESCR = S.TRNSCR_DESCR,
    T.TRNSCR_PRINT_FL = S.TRNSCR_PRINT_FL,
    T.TRNSCR_INDENT = S.TRNSCR_INDENT,
    T.FIRST_TERM_VALID = S.FIRST_TERM_VALID,
    T.CIP_CODE = S.CIP_CODE,
    T.HEGIS_CODE = S.HEGIS_CODE,
    T.TRANSCRIPT_LEVEL = S.TRANSCRIPT_LEVEL,
    T.EVALUATE_SUBPLAN = S.EVALUATE_SUBPLAN,
    T.SSR_LAST_PRS_DT = S.SSR_LAST_PRS_DT,
    T.SSR_LAST_ADM_TERM = S.SSR_LAST_ADM_TERM,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.ACAD_SUBPLAN_TYPE <> S.ACAD_SUBPLAN_TYPE or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.SUBPLN_REQTRM_DFLT <> S.SUBPLN_REQTRM_DFLT or 
    T.DIPLOMA_DESCR <> S.DIPLOMA_DESCR or 
    T.DIPLOMA_PRINT_FL <> S.DIPLOMA_PRINT_FL or 
    T.DIPLOMA_INDENT <> S.DIPLOMA_INDENT or 
    T.TRNSCR_DESCR <> S.TRNSCR_DESCR or 
    T.TRNSCR_PRINT_FL <> S.TRNSCR_PRINT_FL or 
    T.TRNSCR_INDENT <> S.TRNSCR_INDENT or 
    T.FIRST_TERM_VALID <> S.FIRST_TERM_VALID or 
    T.CIP_CODE <> S.CIP_CODE or 
    T.HEGIS_CODE <> S.HEGIS_CODE or 
    T.TRANSCRIPT_LEVEL <> S.TRANSCRIPT_LEVEL or 
    T.EVALUATE_SUBPLAN <> S.EVALUATE_SUBPLAN or 
    nvl(trim(T.SSR_LAST_PRS_DT),0) <> nvl(trim(S.SSR_LAST_PRS_DT),0) or 
    T.SSR_LAST_ADM_TERM <> S.SSR_LAST_ADM_TERM or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.ACAD_PLAN,
    T.ACAD_SUB_PLAN,
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.ACAD_SUBPLAN_TYPE,
    T.DESCR,
    T.DESCRSHORT, 
    T.SUBPLN_REQTRM_DFLT, 
    T.DIPLOMA_DESCR,
    T.DIPLOMA_PRINT_FL, 
    T.DIPLOMA_INDENT, 
    T.TRNSCR_DESCR, 
    T.TRNSCR_PRINT_FL,
    T.TRNSCR_INDENT,
    T.FIRST_TERM_VALID, 
    T.CIP_CODE, 
    T.HEGIS_CODE, 
    T.TRANSCRIPT_LEVEL, 
    T.EVALUATE_SUBPLAN, 
    T.SSR_LAST_PRS_DT,
    T.SSR_LAST_ADM_TERM,
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.INSTITUTION,
    S.ACAD_PLAN,
    S.ACAD_SUB_PLAN,
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.ACAD_SUBPLAN_TYPE,
    S.DESCR,
    S.DESCRSHORT, 
    S.SUBPLN_REQTRM_DFLT, 
    S.DIPLOMA_DESCR,
    S.DIPLOMA_PRINT_FL, 
    S.DIPLOMA_INDENT, 
    S.TRNSCR_DESCR, 
    S.TRNSCR_PRINT_FL,
    S.TRNSCR_INDENT,
    S.FIRST_TERM_VALID, 
    S.CIP_CODE, 
    S.HEGIS_CODE, 
    S.TRANSCRIPT_LEVEL, 
    S.EVALUATE_SUBPLAN, 
    S.SSR_LAST_PRS_DT,
    S.SSR_LAST_ADM_TERM,
    'N',
    'S',
    sysdate,
    sysdate,
    1234);


strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_SUBPLN_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_SUBPLN_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACAD_SUBPLN_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_SUBPLN_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_SUBPLN_TBL';
update CSSTG_OWNER.PS_ACAD_SUBPLN_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, ACAD_PLAN, ACAD_SUB_PLAN, EFFDT
   from CSSTG_OWNER.PS_ACAD_SUBPLN_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_SUBPLN_TBL') = 'Y'
  minus
 select INSTITUTION, ACAD_PLAN, ACAD_SUB_PLAN, EFFDT
   from SYSADM.PS_ACAD_SUBPLN_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_SUBPLN_TBL') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.ACAD_PLAN = S.ACAD_PLAN
   and T.ACAD_SUB_PLAN = S.ACAD_SUB_PLAN
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_SUBPLN_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_SUBPLN_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACAD_SUBPLN_TBL'
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

END PS_ACAD_SUBPLN_TBL_P;
/
