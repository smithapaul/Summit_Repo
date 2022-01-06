CREATE OR REPLACE PROCEDURE             "PS_ACAD_CAR_TBL_P" AUTHID CURRENT_USER IS

/*
-- Run before the first time
DELETE
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACAD_CAR_TBL'

INSERT INTO CSSTG_OWNER.UM_STAGE_JOBS
(TABLE_NAME, DELETE_FLG)
VALUES
('PS_ACAD_CAR_TBL', 'Y')

SELECT *
FROM CSSTG_OWNER.UM_STAGE_JOBS
 WHERE TABLE_NAME = 'PS_ACAD_CAR_TBL'
*/


------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ACAD_CAR_TBL from PeopleSoft table PS_ACAD_CAR_TBL.
--
-- V01  SMT-xxxx 06/05/2017,    Jim Doucette
--                              Converted from PS_ACAD_CAR_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_ACAD_CAR_TBL';
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
 where TABLE_NAME = 'PS_ACAD_CAR_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACAD_CAR_TBL@SASOURCE S)
 where TABLE_NAME = 'PS_ACAD_CAR_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into CSSTG_OWNER.PS_ACAD_CAR_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_ACAD_CAR_TBL';
merge /*+ use_hash(S,T) */ into CSSTG_OWNER.PS_ACAD_CAR_TBL T 
using (select /*+ full(S) */
    nvl(trim(INSTITUTION),'-') INSTITUTION, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(GRADING_SCHEME),'-') GRADING_SCHEME, 
    nvl(trim(GRADING_BASIS),'-') GRADING_BASIS, 
    nvl(trim(GRADE_TRANSFER),'-') GRADE_TRANSFER, 
    nvl(trim(REPEAT_SCHEME),'-') REPEAT_SCHEME, 
    nvl(trim(TERM_TYPE),'-') TERM_TYPE, 
    nvl(trim(HOLIDAY_SCHEDULE),'-') HOLIDAY_SCHEDULE, 
    nvl(FA_PRIMACY_NBR,0) FA_PRIMACY_NBR, 
    nvl(trim(ACAD_PLAN_TYPE),'-') ACAD_PLAN_TYPE, 
    nvl(trim(ADVISOR_EDIT),'-') ADVISOR_EDIT, 
    nvl(trim(LST_TRM_HIST_ENRL),'-') LST_TRM_HIST_ENRL, 
    nvl(trim(DYN_CLASS_DATA),'-') DYN_CLASS_DATA, 
    nvl(trim(OEE_DYN_DATE_RULE),'-') OEE_DYN_DATE_RULE, 
    nvl(trim(USE_DYN_CLASS_DATE),'-') USE_DYN_CLASS_DATE, 
    nvl(trim(SF_GRAD_DESIGNATIO),'-') SF_GRAD_DESIGNATIO, 
    nvl(trim(FA_CAR_TYPE),'-') FA_CAR_TYPE, 
    nvl(trim(GRADUATE_LVL_IND),'-') GRADUATE_LVL_IND, 
    nvl(trim(OEE_IND),'-') OEE_IND, 
    nvl(trim(REPEAT_RULE),'-') REPEAT_RULE, 
    nvl(trim(REPEAT_ENRL_CTL),'-') REPEAT_ENRL_CTL, 
    nvl(trim(REPEAT_ENRL_SUSP),'-') REPEAT_ENRL_SUSP, 
    nvl(trim(REPEAT_GRD_CK),'-') REPEAT_GRD_CK, 
    nvl(trim(REPEAT_GRD_SUSP),'-') REPEAT_GRD_SUSP, 
    nvl(trim(REPEAT_CRSE_ERROR),'-') REPEAT_CRSE_ERROR, 
    nvl(trim(SS_ENRL_APPT_CHKPT),'-') SS_ENRL_APPT_CHKPT, 
    nvl(trim(SSR_ALLOW_PROG_IN),'-') SSR_ALLOW_PROG_IN, 
    nvl(trim(SSR_DFLT_TRMAC_LST),'-') SSR_DFLT_TRMAC_LST, 
    nvl(trim(SAA_DISPLAY_OPTION),'-') SAA_DISPLAY_OPTION
from SYSADM.PS_ACAD_CAR_TBL@SASOURCE S
where ORA_ROWSCN > (select OLD_MAX_SCN from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_CAR_TBL') ) S 
on ( 
    T.INSTITUTION = S.INSTITUTION and 
    T.ACAD_CAREER = S.ACAD_CAREER and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90')
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.GRADING_SCHEME = S.GRADING_SCHEME,
    T.GRADING_BASIS = S.GRADING_BASIS,
    T.GRADE_TRANSFER = S.GRADE_TRANSFER,
    T.REPEAT_SCHEME = S.REPEAT_SCHEME,
    T.TERM_TYPE = S.TERM_TYPE,
    T.HOLIDAY_SCHEDULE = S.HOLIDAY_SCHEDULE,
    T.FA_PRIMACY_NBR = S.FA_PRIMACY_NBR,
    T.ACAD_PLAN_TYPE = S.ACAD_PLAN_TYPE,
    T.ADVISOR_EDIT = S.ADVISOR_EDIT,
    T.LST_TRM_HIST_ENRL = S.LST_TRM_HIST_ENRL,
    T.DYN_CLASS_DATA = S.DYN_CLASS_DATA,
    T.OEE_DYN_DATE_RULE = S.OEE_DYN_DATE_RULE,
    T.USE_DYN_CLASS_DATE = S.USE_DYN_CLASS_DATE,
    T.SF_GRAD_DESIGNATIO = S.SF_GRAD_DESIGNATIO,
    T.FA_CAR_TYPE = S.FA_CAR_TYPE,
    T.GRADUATE_LVL_IND = S.GRADUATE_LVL_IND,
    T.OEE_IND = S.OEE_IND,
    T.REPEAT_RULE = S.REPEAT_RULE,
    T.REPEAT_ENRL_CTL = S.REPEAT_ENRL_CTL,
    T.REPEAT_ENRL_SUSP = S.REPEAT_ENRL_SUSP,
    T.REPEAT_GRD_CK = S.REPEAT_GRD_CK,
    T.REPEAT_GRD_SUSP = S.REPEAT_GRD_SUSP,
    T.REPEAT_CRSE_ERROR = S.REPEAT_CRSE_ERROR,
    T.SS_ENRL_APPT_CHKPT = S.SS_ENRL_APPT_CHKPT,
    T.SSR_ALLOW_PROG_IN = S.SSR_ALLOW_PROG_IN,
    T.SSR_DFLT_TRMAC_LST = S.SSR_DFLT_TRMAC_LST,
    T.SAA_DISPLAY_OPTION = S.SAA_DISPLAY_OPTION,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.GRADING_SCHEME <> S.GRADING_SCHEME or 
    T.GRADING_BASIS <> S.GRADING_BASIS or 
    T.GRADE_TRANSFER <> S.GRADE_TRANSFER or 
    T.REPEAT_SCHEME <> S.REPEAT_SCHEME or 
    T.TERM_TYPE <> S.TERM_TYPE or 
    T.HOLIDAY_SCHEDULE <> S.HOLIDAY_SCHEDULE or 
    T.FA_PRIMACY_NBR <> S.FA_PRIMACY_NBR or 
    T.ACAD_PLAN_TYPE <> S.ACAD_PLAN_TYPE or 
    T.ADVISOR_EDIT <> S.ADVISOR_EDIT or 
    T.LST_TRM_HIST_ENRL <> S.LST_TRM_HIST_ENRL or 
    T.DYN_CLASS_DATA <> S.DYN_CLASS_DATA or 
    T.OEE_DYN_DATE_RULE <> S.OEE_DYN_DATE_RULE or 
    T.USE_DYN_CLASS_DATE <> S.USE_DYN_CLASS_DATE or 
    T.SF_GRAD_DESIGNATIO <> S.SF_GRAD_DESIGNATIO or 
    T.FA_CAR_TYPE <> S.FA_CAR_TYPE or 
    T.GRADUATE_LVL_IND <> S.GRADUATE_LVL_IND or 
    T.OEE_IND <> S.OEE_IND or 
    T.REPEAT_RULE <> S.REPEAT_RULE or 
    T.REPEAT_ENRL_CTL <> S.REPEAT_ENRL_CTL or 
    T.REPEAT_ENRL_SUSP <> S.REPEAT_ENRL_SUSP or 
    T.REPEAT_GRD_CK <> S.REPEAT_GRD_CK or 
    T.REPEAT_GRD_SUSP <> S.REPEAT_GRD_SUSP or 
    T.REPEAT_CRSE_ERROR <> S.REPEAT_CRSE_ERROR or 
    T.SS_ENRL_APPT_CHKPT <> S.SS_ENRL_APPT_CHKPT or 
    T.SSR_ALLOW_PROG_IN <> S.SSR_ALLOW_PROG_IN or 
    T.SSR_DFLT_TRMAC_LST <> S.SSR_DFLT_TRMAC_LST or 
    T.SAA_DISPLAY_OPTION <> S.SAA_DISPLAY_OPTION or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.ACAD_CAREER,
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.DESCR,
    T.DESCRSHORT, 
    T.GRADING_SCHEME, 
    T.GRADING_BASIS,
    T.GRADE_TRANSFER, 
    T.REPEAT_SCHEME,
    T.TERM_TYPE,
    T.HOLIDAY_SCHEDULE, 
    T.FA_PRIMACY_NBR, 
    T.ACAD_PLAN_TYPE, 
    T.ADVISOR_EDIT, 
    T.LST_TRM_HIST_ENRL,
    T.DYN_CLASS_DATA, 
    T.OEE_DYN_DATE_RULE,
    T.USE_DYN_CLASS_DATE, 
    T.SF_GRAD_DESIGNATIO, 
    T.FA_CAR_TYPE,
    T.GRADUATE_LVL_IND, 
    T.OEE_IND,
    T.REPEAT_RULE,
    T.REPEAT_ENRL_CTL,
    T.REPEAT_ENRL_SUSP, 
    T.REPEAT_GRD_CK,
    T.REPEAT_GRD_SUSP,
    T.REPEAT_CRSE_ERROR,
    T.SS_ENRL_APPT_CHKPT, 
    T.SSR_ALLOW_PROG_IN,
    T.SSR_DFLT_TRMAC_LST, 
    T.SAA_DISPLAY_OPTION, 
    T.LOAD_ERROR, 
    T.DATA_ORIGIN,
    T.CREATED_EW_DTTM,
    T.LASTUPD_EW_DTTM,
    T.BATCH_SID
    ) 
values (
    S.INSTITUTION,
    S.ACAD_CAREER,
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.DESCR,
    S.DESCRSHORT, 
    S.GRADING_SCHEME, 
    S.GRADING_BASIS,
    S.GRADE_TRANSFER, 
    S.REPEAT_SCHEME,
    S.TERM_TYPE,
    S.HOLIDAY_SCHEDULE, 
    S.FA_PRIMACY_NBR, 
    S.ACAD_PLAN_TYPE, 
    S.ADVISOR_EDIT, 
    S.LST_TRM_HIST_ENRL,
    S.DYN_CLASS_DATA, 
    S.OEE_DYN_DATE_RULE,
    S.USE_DYN_CLASS_DATE, 
    S.SF_GRAD_DESIGNATIO, 
    S.FA_CAR_TYPE,
    S.GRADUATE_LVL_IND, 
    S.OEE_IND,
    S.REPEAT_RULE,
    S.REPEAT_ENRL_CTL,
    S.REPEAT_ENRL_SUSP, 
    S.REPEAT_GRD_CK,
    S.REPEAT_GRD_SUSP,
    S.REPEAT_CRSE_ERROR,
    S.SS_ENRL_APPT_CHKPT, 
    S.SSR_ALLOW_PROG_IN,
    S.SSR_DFLT_TRMAC_LST, 
    S.SAA_DISPLAY_OPTION, 
    'N',
    'S',
    sysdate,
    sysdate,
    1234);

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_CAR_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_CAR_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on CSSTG_OWNER.UM_STAGE_JOBS';
update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACAD_CAR_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_CAR_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_ACAD_CAR_TBL';
update CSSTG_OWNER.PS_ACAD_CAR_TBL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, ACAD_CAREER, EFFDT
   from CSSTG_OWNER.PS_ACAD_CAR_TBL T2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_CAR_TBL') = 'Y'
  minus
 select INSTITUTION, ACAD_CAREER, EFFDT
   from SYSADM.PS_ACAD_CAR_TBL@SASOURCE S2
  where (select DELETE_FLG from CSSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_CAR_TBL') = 'Y'
   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.ACAD_CAREER = S.ACAD_CAREER
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_CAR_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_CAR_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating CSSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on CSSTG_OWNER.UM_STAGE_JOBS';

update CSSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACAD_CAR_TBL'
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

END PS_ACAD_CAR_TBL_P;
/
