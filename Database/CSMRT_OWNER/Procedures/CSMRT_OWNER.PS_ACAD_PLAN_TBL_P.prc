CREATE OR REPLACE PROCEDURE             PS_ACAD_PLAN_TBL_P AUTHID CURRENT_USER IS

/*
-- Run before the first time

DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACAD_PLAN_TBL'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_ACAD_PLAN_TBL', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACAD_PLAN_TBL'

*/ 

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ACAD_PLAN_TBL from PeopleSoft table PS_ACAD_PLAN_TBL.
--
-- V01  SMT-xxxx 8/18/2017,    Preethi Lodha
--                             Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ACAD_PLAN_TBL';
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
 where TABLE_NAME = 'PS_ACAD_PLAN_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Truncating',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACAD_PLAN_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_ACAD_PLAN_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlDynamic   := 'truncate table CSSTG_OWNER.PS_T_ACAD_PLAN_TBL';
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
 where TABLE_NAME = 'PS_ACAD_PLAN_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand := 'insert';
INSERT /*+ append */
      INTO  CSSTG_OWNER.PS_T_ACAD_PLAN_TBL
   SELECT /*+ full(S) */
        INSTITUTION,
         ACAD_PLAN,
        EFFDT,         
          'CS90' SRC_SYS_ID,
          EFF_STATUS,
          DESCR,
          DESCRSHORT,
          ACAD_PLAN_TYPE,
          ACAD_PROG,
          PLN_REQTRM_DFLT,
          DEGREE,
          DIPLOMA_DESCR,
          DIPLOMA_PRINT_FL,
          DIPLOMA_INDENT,
          TRNSCR_DESCR,
          TRNSCR_PRINT_FL,
          TRNSCR_INDENT,
          FIRST_TERM_VALID,
          CIP_CODE,
          HEGIS_CODE,
          ACAD_CAREER,
          TRANSCRIPT_LEVEL,
          STUDY_FIELD,
          EVALUATE_PLAN,
          SSR_LAST_PRS_DT,
          SSR_LAST_ADM_TERM,
          SAA_WHIF_DISP_ADVR,
          SAA_WHIF_DISP_PREM,
          SAA_WHIF_DISP_STD,
          SSR_NSC_CRD_LVL,
          SSR_PROG_LEN_TYPE,
          SSR_PROG_LENGTH,
          SFA_SPEC_PROG_FLG,
          SSR_NSC_INCL_PLAN,
          to_char(substr(trim(DESCRLONG), 1, 4000))  DESCRLONG,
    to_number(ORA_ROWSCN) SRC_SCN
from SYSADM.PS_ACAD_PLAN_TBL@SASOURCE S
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging'
 where TABLE_NAME = 'PS_ACAD_PLAN_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ACAD_PLAN_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ACAD_PLAN_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ACAD_PLAN_TBL T
using (select /*+ full(S) */
nvl(trim(INSTITUTION),'-') INSTITUTION,
nvl(trim(ACAD_PLAN),'-') ACAD_PLAN,
to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,
nvl(trim(EFF_STATUS),'-') EFF_STATUS,
nvl(trim(DESCR),'-') DESCR,
nvl(trim(DESCRSHORT),'-') DESCRSHORT,
nvl(trim(ACAD_PLAN_TYPE),'-') ACAD_PLAN_TYPE,
nvl(trim(ACAD_PROG),'-') ACAD_PROG,
nvl(trim(PLN_REQTRM_DFLT),'-') PLN_REQTRM_DFLT,
nvl(trim(DEGREE),'-') DEGREE,
nvl(trim(DIPLOMA_DESCR),'-') DIPLOMA_DESCR,
nvl(trim(DIPLOMA_PRINT_FL),'-') DIPLOMA_PRINT_FL,
nvl(DIPLOMA_INDENT,0) DIPLOMA_INDENT,
nvl(trim(TRNSCR_DESCR),'-') TRNSCR_DESCR,
nvl(trim(TRNSCR_PRINT_FL),'-') TRNSCR_PRINT_FL,
nvl(TRNSCR_INDENT,0) TRNSCR_INDENT,
nvl(trim(FIRST_TERM_VALID),'-') FIRST_TERM_VALID,
nvl(trim(CIP_CODE),'-') CIP_CODE,
nvl(trim(HEGIS_CODE),'-') HEGIS_CODE,
nvl(trim(ACAD_CAREER),'-') ACAD_CAREER,
nvl(trim(TRANSCRIPT_LEVEL),'-') TRANSCRIPT_LEVEL,
nvl(trim(STUDY_FIELD),'-') STUDY_FIELD,
nvl(trim(EVALUATE_PLAN),'-') EVALUATE_PLAN,
to_date(to_char(case when SSR_LAST_PRS_DT < '01-JAN-1800' then NULL else SSR_LAST_PRS_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SSR_LAST_PRS_DT,
SSR_LAST_ADM_TERM SSR_LAST_ADM_TERM,
SAA_WHIF_DISP_ADVR SAA_WHIF_DISP_ADVR,
SAA_WHIF_DISP_PREM SAA_WHIF_DISP_PREM,
SAA_WHIF_DISP_STD SAA_WHIF_DISP_STD,
SSR_NSC_CRD_LVL SSR_NSC_CRD_LVL,
SSR_PROG_LEN_TYPE SSR_PROG_LEN_TYPE,
SSR_PROG_LENGTH SSR_PROG_LENGTH,
SFA_SPEC_PROG_FLG SFA_SPEC_PROG_FLG,
SSR_NSC_INCL_PLAN SSR_NSC_INCL_PLAN,
DESCRLONG DESCRLONG
from CSSTG_OWNER.PS_T_ACAD_PLAN_TBL S
where SRC_SCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_PLAN_TBL') ) S
   on (
T.INSTITUTION = S.INSTITUTION and
T.ACAD_PLAN = S.ACAD_PLAN and
T.EFFDT = S.EFFDT and
T.SRC_SYS_ID = 'CS90')
when matched then update set
T.EFF_STATUS = S.EFF_STATUS,
T.DESCR = S.DESCR,
T.DESCRSHORT = S.DESCRSHORT,
T.ACAD_PLAN_TYPE = S.ACAD_PLAN_TYPE,
T.ACAD_PROG = S.ACAD_PROG,
T.PLN_REQTRM_DFLT = S.PLN_REQTRM_DFLT,
T.DEGREE = S.DEGREE,
T.DIPLOMA_DESCR = S.DIPLOMA_DESCR,
T.DIPLOMA_PRINT_FL = S.DIPLOMA_PRINT_FL,
T.DIPLOMA_INDENT = S.DIPLOMA_INDENT,
T.TRNSCR_DESCR = S.TRNSCR_DESCR,
T.TRNSCR_PRINT_FL = S.TRNSCR_PRINT_FL,
T.TRNSCR_INDENT = S.TRNSCR_INDENT,
T.FIRST_TERM_VALID = S.FIRST_TERM_VALID,
T.CIP_CODE = S.CIP_CODE,
T.HEGIS_CODE = S.HEGIS_CODE,
T.ACAD_CAREER = S.ACAD_CAREER,
T.TRANSCRIPT_LEVEL = S.TRANSCRIPT_LEVEL,
T.STUDY_FIELD = S.STUDY_FIELD,
T.EVALUATE_PLAN = S.EVALUATE_PLAN,
T.SSR_LAST_PRS_DT = S.SSR_LAST_PRS_DT,
T.SSR_LAST_ADM_TERM = S.SSR_LAST_ADM_TERM,
T.SAA_WHIF_DISP_ADVR = S.SAA_WHIF_DISP_ADVR,
T.SAA_WHIF_DISP_PREM = S.SAA_WHIF_DISP_PREM,
T.SAA_WHIF_DISP_STD = S.SAA_WHIF_DISP_STD,
T.SSR_NSC_CRD_LVL = S.SSR_NSC_CRD_LVL,
T.SSR_PROG_LEN_TYPE = S.SSR_PROG_LEN_TYPE,
T.SSR_PROG_LENGTH = S.SSR_PROG_LENGTH,
T.SFA_SPEC_PROG_FLG = S.SFA_SPEC_PROG_FLG,
T.SSR_NSC_INCL_PLAN = S.SSR_NSC_INCL_PLAN,
T.DESCRLONG = S.DESCRLONG,
T.DATA_ORIGIN = 'S',
T.LASTUPD_EW_DTTM = sysdate,
T.BATCH_SID   = 1234
where
T.EFF_STATUS <> S.EFF_STATUS or
T.DESCR <> S.DESCR or
T.DESCRSHORT <> S.DESCRSHORT or
T.ACAD_PLAN_TYPE <> S.ACAD_PLAN_TYPE or
T.ACAD_PROG <> S.ACAD_PROG or
T.PLN_REQTRM_DFLT <> S.PLN_REQTRM_DFLT or
T.DEGREE <> S.DEGREE or
T.DIPLOMA_DESCR <> S.DIPLOMA_DESCR or
T.DIPLOMA_PRINT_FL <> S.DIPLOMA_PRINT_FL or
T.DIPLOMA_INDENT <> S.DIPLOMA_INDENT or
T.TRNSCR_DESCR <> S.TRNSCR_DESCR or
T.TRNSCR_PRINT_FL <> S.TRNSCR_PRINT_FL or
T.TRNSCR_INDENT <> S.TRNSCR_INDENT or
T.FIRST_TERM_VALID <> S.FIRST_TERM_VALID or
T.CIP_CODE <> S.CIP_CODE or
T.HEGIS_CODE <> S.HEGIS_CODE or
T.ACAD_CAREER <> S.ACAD_CAREER or
T.TRANSCRIPT_LEVEL <> S.TRANSCRIPT_LEVEL or
T.STUDY_FIELD <> S.STUDY_FIELD or
T.EVALUATE_PLAN <> S.EVALUATE_PLAN or
nvl(trim(T.SSR_LAST_PRS_DT),0) <> nvl(trim(S.SSR_LAST_PRS_DT),0) or
nvl(trim(T.SSR_LAST_ADM_TERM),0) <> nvl(trim(S.SSR_LAST_ADM_TERM),0) or
nvl(trim(T.SAA_WHIF_DISP_ADVR),0) <> nvl(trim(S.SAA_WHIF_DISP_ADVR),0) or
nvl(trim(T.SAA_WHIF_DISP_PREM),0) <> nvl(trim(S.SAA_WHIF_DISP_PREM),0) or
nvl(trim(T.SAA_WHIF_DISP_STD),0) <> nvl(trim(S.SAA_WHIF_DISP_STD),0) or
nvl(trim(T.SSR_NSC_CRD_LVL),0) <> nvl(trim(S.SSR_NSC_CRD_LVL),0) or
nvl(trim(T.SSR_PROG_LEN_TYPE),0) <> nvl(trim(S.SSR_PROG_LEN_TYPE),0) or
nvl(trim(T.SSR_PROG_LENGTH),0) <> nvl(trim(S.SSR_PROG_LENGTH),0) or
nvl(trim(T.SFA_SPEC_PROG_FLG),0) <> nvl(trim(S.SFA_SPEC_PROG_FLG),0) or
nvl(trim(T.SSR_NSC_INCL_PLAN),0) <> nvl(trim(S.SSR_NSC_INCL_PLAN),0) or
nvl(trim(T.DESCRLONG),0) <> nvl(trim(S.DESCRLONG),0) or
T.DATA_ORIGIN = 'D'
when not matched then
insert (
T.INSTITUTION,
T.ACAD_PLAN,
T.EFFDT,
T.SRC_SYS_ID,
T.EFF_STATUS,
T.DESCR,
T.DESCRSHORT,
T.ACAD_PLAN_TYPE,
T.ACAD_PROG,
T.PLN_REQTRM_DFLT,
T.DEGREE,
T.DIPLOMA_DESCR,
T.DIPLOMA_PRINT_FL,
T.DIPLOMA_INDENT,
T.TRNSCR_DESCR,
T.TRNSCR_PRINT_FL,
T.TRNSCR_INDENT,
T.FIRST_TERM_VALID,
T.CIP_CODE,
T.HEGIS_CODE,
T.ACAD_CAREER,
T.TRANSCRIPT_LEVEL,
T.STUDY_FIELD,
T.EVALUATE_PLAN,
T.SSR_LAST_PRS_DT,
T.SSR_LAST_ADM_TERM,
T.SAA_WHIF_DISP_ADVR,
T.SAA_WHIF_DISP_PREM,
T.SAA_WHIF_DISP_STD,
T.SSR_NSC_CRD_LVL,
T.SSR_PROG_LEN_TYPE,
T.SSR_PROG_LENGTH,
T.SFA_SPEC_PROG_FLG,
T.SSR_NSC_INCL_PLAN,
T.LOAD_ERROR,
T.DATA_ORIGIN,
T.CREATED_EW_DTTM,
T.LASTUPD_EW_DTTM,
T.BATCH_SID,
T.DESCRLONG
)
values (
S.INSTITUTION,
S.ACAD_PLAN,
S.EFFDT,
'CS90',
S.EFF_STATUS,
S.DESCR,
S.DESCRSHORT,
S.ACAD_PLAN_TYPE,
S.ACAD_PROG,
S.PLN_REQTRM_DFLT,
S.DEGREE,
S.DIPLOMA_DESCR,
S.DIPLOMA_PRINT_FL,
S.DIPLOMA_INDENT,
S.TRNSCR_DESCR,
S.TRNSCR_PRINT_FL,
S.TRNSCR_INDENT,
S.FIRST_TERM_VALID,
S.CIP_CODE,
S.HEGIS_CODE,
S.ACAD_CAREER,
S.TRANSCRIPT_LEVEL,
S.STUDY_FIELD,
S.EVALUATE_PLAN,
S.SSR_LAST_PRS_DT,
S.SSR_LAST_ADM_TERM,
S.SAA_WHIF_DISP_ADVR,
S.SAA_WHIF_DISP_PREM,
S.SAA_WHIF_DISP_STD,
S.SSR_NSC_CRD_LVL,
S.SSR_PROG_LEN_TYPE,
S.SSR_PROG_LENGTH,
S.SFA_SPEC_PROG_FLG,
S.SSR_NSC_INCL_PLAN,
'N',
'S',
sysdate,
sysdate,
1234,
S.DESCRLONG);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;


strMessage01    := '# of PS_ACAD_PLAN_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_PLAN_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACAD_PLAN_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_PLAN_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_PLAN_TBL';
update CSSTG_OWNER.PS_ACAD_PLAN_TBL T
   set T.DATA_ORIGIN = 'D',
       T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, ACAD_PLAN, EFFDT
   from CSSTG_OWNER.PS_ACAD_PLAN_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_PLAN_TBL') = 'Y'
  minus
 select INSTITUTION, ACAD_PLAN, EFFDT
   from SYSADM.PS_ACAD_PLAN_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_PLAN_TBL') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.ACAD_PLAN = S.ACAD_PLAN
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_PLAN_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_PLAN_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACAD_PLAN_TBL'
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

END PS_ACAD_PLAN_TBL_P;
/
