DROP PROCEDURE CSMRT_OWNER.AM_PS_ACAD_PROG_TBL_P
/

--
-- AM_PS_ACAD_PROG_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_ACAD_PROG_TBL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_ACAD_PROG_TBL from PeopleSoft table PS_ACAD_PROG_TBL.
--
-- V01  SMT-xxxx 04/21/2017,    Jim Doucette
--                              Converted from PS_ACAD_PROG_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_ACAD_PROG_TBL';
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
 where TABLE_NAME = 'PS_ACAD_PROG_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_ACAD_PROG_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_ACAD_PROG_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_ACAD_PROG_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_ACAD_PROG_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_ACAD_PROG_TBL T                  
using (select /*+ full(S) */                                                    
    nvl(trim(INSTITUTION),'-') INSTITUTION,                                         
    nvl(trim(ACAD_PROG),'-') ACAD_PROG,                                             
    to_date(to_char(case when EFFDT < '01-JAN-1800' then NULL 
                    else EFFDT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT, 
    nvl(trim(EFF_STATUS),'-') EFF_STATUS, 
    nvl(trim(DESCR),'-') DESCR, 
    nvl(trim(DESCRSHORT),'-') DESCRSHORT, 
    nvl(trim(ACAD_CAREER),'-') ACAD_CAREER, 
    nvl(trim(ACAD_CALENDAR_ID),'-') ACAD_CALENDAR_ID, 
    nvl(trim(ADVISOR_EDIT),'-') ADVISOR_EDIT, 
    nvl(trim(LEVEL_LOAD_RULE),'-') LEVEL_LOAD_RULE, 
    nvl(trim(ACAD_GROUP),'-') ACAD_GROUP, 
    nvl(trim(ACAD_PLAN),'-') ACAD_PLAN, 
    nvl(trim(CAMPUS),'-') CAMPUS, 
    nvl(trim(FIRST_TERM_VALID),'-') FIRST_TERM_VALID, 
    nvl(trim(CAR_PTR_EXC_RULE),'-') CAR_PTR_EXC_RULE, 
    nvl(trim(CAR_PTR_EXC_FG),'-') CAR_PTR_EXC_FG, 
    nvl(FA_PRIMACY_NBR,0) FA_PRIMACY_NBR, 
    nvl(trim(FA_ELIGIBILITY),'-') FA_ELIGIBILITY, 
    nvl(PROG_NORM_COMPLTN,0) PROG_NORM_COMPLTN, 
    nvl(trim(RESIDENCY_REQ),'-') RESIDENCY_REQ, 
    nvl(trim(CIP_CODE),'-') CIP_CODE, 
    nvl(trim(HEGIS_CODE),'-') HEGIS_CODE, 
    nvl(trim(CRSE_COUNT_ENRL),'-') CRSE_COUNT_ENRL, 
    nvl(CRSE_COUNT_MIN,0) CRSE_COUNT_MIN, 
    nvl(trim(ACAD_ORG),'-') ACAD_ORG, 
    nvl(trim(SPLIT_OWNER),'-') SPLIT_OWNER, 
    nvl(trim(ACAD_PROG_DUAL),'-') ACAD_PROG_DUAL, 
    nvl(trim(GRADING_SCHEME),'-') GRADING_SCHEME, 
    nvl(trim(GRADING_BASIS),'-') GRADING_BASIS, 
    nvl(trim(GRADE_TRANSFER),'-') GRADE_TRANSFER, 
    nvl(trim(TRANSCRIPT_LEVEL),'-') TRANSCRIPT_LEVEL, 
    nvl(trim(ACAD_STDNG_RULE),'-') ACAD_STDNG_RULE, 
    nvl(trim(ASSOC_PROG_AS),'-') ASSOC_PROG_AS, 
    nvl(trim(CALC_AS_BATCH_ONLY),'-') CALC_AS_BATCH_ONLY, 
    nvl(trim(OBEY_FULLY_GRD_AS),'-') OBEY_FULLY_GRD_AS, 
    nvl(trim(EXCL_TRM_CAT_AS_1),'-') EXCL_TRM_CAT_AS_1, 
    nvl(trim(EXCL_TRM_CAT_AS_2),'-') EXCL_TRM_CAT_AS_2, 
    nvl(trim(EXCL_TRM_CAT_AS_3),'-') EXCL_TRM_CAT_AS_3, 
    nvl(trim(HONOR_AWARD_RULE),'-') HONOR_AWARD_RULE, 
    nvl(trim(ASSOC_PROG_HA),'-') ASSOC_PROG_HA, 
    nvl(trim(CALC_HA_BATCH_ONLY),'-') CALC_HA_BATCH_ONLY, 
    nvl(trim(OBEY_FULLY_GRD_HA),'-') OBEY_FULLY_GRD_HA, 
    nvl(trim(EXCL_TRM_CAT_HA_1),'-') EXCL_TRM_CAT_HA_1, 
    nvl(trim(EXCL_TRM_CAT_HA_2),'-') EXCL_TRM_CAT_HA_2, 
    nvl(trim(EXCL_TRM_CAT_HA_3),'-') EXCL_TRM_CAT_HA_3, 
    nvl(trim(HONOR_DT_FG),'-') HONOR_DT_FG, 
    nvl(trim(INCOMPLETE_GRADE),'-') INCOMPLETE_GRADE, 
    nvl(trim(LAPSE_GRADE),'-') LAPSE_GRADE, 
    nvl(trim(LAPSE_TO_GRADE),'-') LAPSE_TO_GRADE, 
    nvl(LAPSE_DAYS,0) LAPSE_DAYS, 
    nvl(trim(LAPSE_NOTE_ID),'-') LAPSE_NOTE_ID, 
    nvl(trim(PRINT_LAPSE_DATE),'-') PRINT_LAPSE_DATE, 
    nvl(trim(CMPLTD_NOTE_ID),'-') CMPLTD_NOTE_ID, 
    nvl(trim(PRINT_CMPLTD_DATE),'-') PRINT_CMPLTD_DATE, 
    nvl(trim(REPEAT_RULE),'-') REPEAT_RULE, 
    nvl(trim(REPEAT_GRD_CK),'-') REPEAT_GRD_CK, 
    nvl(trim(CANCEL_REASON),'-') CANCEL_REASON, 
    nvl(trim(WD_WO_PEN_REASON),'-') WD_WO_PEN_REASON, 
    nvl(trim(WD_W_PEN_GRD_BAS),'-') WD_W_PEN_GRD_BAS, 
    nvl(trim(WD_W_PEN_GRADE),'-') WD_W_PEN_GRADE, 
    nvl(trim(WD_W_PEN2_GRADE),'-') WD_W_PEN2_GRADE, 
    nvl(trim(WD_W_PEN2_GRD_BAS),'-') WD_W_PEN2_GRD_BAS, 
    nvl(trim(DROP_RET_RSN),'-') DROP_RET_RSN, 
    nvl(trim(DROP_PEN_GRADE),'-') DROP_PEN_GRADE, 
    nvl(trim(DROP_PEN_GRADE_2),'-') DROP_PEN_GRADE_2, 
    nvl(trim(DROP_PEN_GRD_BAS),'-') DROP_PEN_GRD_BAS, 
    nvl(trim(DROP_PEN_GRD_BAS_2),'-') DROP_PEN_GRD_BAS_2, 
    nvl(trim(OEE_IND),'-') OEE_IND, 
    nvl(trim(REPEAT_ENRL_CTL),'-') REPEAT_ENRL_CTL, 
    nvl(trim(REPEAT_ENRL_SUSP),'-') REPEAT_ENRL_SUSP, 
    nvl(trim(REPEAT_GRD_SUSP),'-') REPEAT_GRD_SUSP, 
    nvl(trim(REPEAT_CRSE_ERROR),'-') REPEAT_CRSE_ERROR, 
    to_date(to_char(case when SSR_LAST_PRS_DT < '01-JAN-1800' then NULL 
                    else SSR_LAST_PRS_DT end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') SSR_LAST_PRS_DT, 
    nvl(trim(SSR_LAST_ADM_TERM),'-') SSR_LAST_ADM_TERM
from SYSADM.PS_ACAD_PROG_TBL@AMSOURCE S 
where ORA_ROWSCN > (select OLD_MAX_SCN from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_PROG_TBL') ) S
 on ( 
    T.INSTITUTION = S.INSTITUTION and 
    T.ACAD_PROG = S.ACAD_PROG and 
    T.EFFDT = S.EFFDT and 
    T.SRC_SYS_ID = 'CS90'
    )
when matched then update set
    T.EFF_STATUS = S.EFF_STATUS,
    T.DESCR = S.DESCR,
    T.DESCRSHORT = S.DESCRSHORT,
    T.ACAD_CAREER = S.ACAD_CAREER,
    T.ACAD_CALENDAR_ID = S.ACAD_CALENDAR_ID,
    T.ADVISOR_EDIT = S.ADVISOR_EDIT,
    T.LEVEL_LOAD_RULE = S.LEVEL_LOAD_RULE,
    T.ACAD_GROUP = S.ACAD_GROUP,
    T.ACAD_PLAN = S.ACAD_PLAN,
    T.CAMPUS = S.CAMPUS,
    T.FIRST_TERM_VALID = S.FIRST_TERM_VALID,
    T.CAR_PTR_EXC_RULE = S.CAR_PTR_EXC_RULE,
    T.CAR_PTR_EXC_FG = S.CAR_PTR_EXC_FG,
    T.FA_PRIMACY_NBR = S.FA_PRIMACY_NBR,
    T.FA_ELIGIBILITY = S.FA_ELIGIBILITY,
    T.PROG_NORM_COMPLTN = S.PROG_NORM_COMPLTN,
    T.RESIDENCY_REQ = S.RESIDENCY_REQ,
    T.CIP_CODE = S.CIP_CODE,
    T.HEGIS_CODE = S.HEGIS_CODE,
    T.CRSE_COUNT_ENRL = S.CRSE_COUNT_ENRL,
    T.CRSE_COUNT_MIN = S.CRSE_COUNT_MIN,
    T.ACAD_ORG = S.ACAD_ORG,
    T.SPLIT_OWNER = S.SPLIT_OWNER,
    T.ACAD_PROG_DUAL = S.ACAD_PROG_DUAL,
    T.GRADING_SCHEME = S.GRADING_SCHEME,
    T.GRADING_BASIS = S.GRADING_BASIS,
    T.GRADE_TRANSFER = S.GRADE_TRANSFER,
    T.TRANSCRIPT_LEVEL = S.TRANSCRIPT_LEVEL,
    T.ACAD_STDNG_RULE = S.ACAD_STDNG_RULE,
    T.ASSOC_PROG_AS = S.ASSOC_PROG_AS,
    T.CALC_AS_BATCH_ONLY = S.CALC_AS_BATCH_ONLY,
    T.OBEY_FULLY_GRD_AS = S.OBEY_FULLY_GRD_AS,
    T.EXCL_TRM_CAT_AS_1 = S.EXCL_TRM_CAT_AS_1,
    T.EXCL_TRM_CAT_AS_2 = S.EXCL_TRM_CAT_AS_2,
    T.EXCL_TRM_CAT_AS_3 = S.EXCL_TRM_CAT_AS_3,
    T.HONOR_AWARD_RULE = S.HONOR_AWARD_RULE,
    T.ASSOC_PROG_HA = S.ASSOC_PROG_HA,
    T.CALC_HA_BATCH_ONLY = S.CALC_HA_BATCH_ONLY,
    T.OBEY_FULLY_GRD_HA = S.OBEY_FULLY_GRD_HA,
    T.EXCL_TRM_CAT_HA_1 = S.EXCL_TRM_CAT_HA_1,
    T.EXCL_TRM_CAT_HA_2 = S.EXCL_TRM_CAT_HA_2,
    T.EXCL_TRM_CAT_HA_3 = S.EXCL_TRM_CAT_HA_3,
    T.HONOR_DT_FG = S.HONOR_DT_FG,
    T.INCOMPLETE_GRADE = S.INCOMPLETE_GRADE,
    T.LAPSE_GRADE = S.LAPSE_GRADE,
    T.LAPSE_TO_GRADE = S.LAPSE_TO_GRADE,
    T.LAPSE_DAYS = S.LAPSE_DAYS,
    T.LAPSE_NOTE_ID = S.LAPSE_NOTE_ID,
    T.PRINT_LAPSE_DATE = S.PRINT_LAPSE_DATE,
    T.CMPLTD_NOTE_ID = S.CMPLTD_NOTE_ID,
    T.PRINT_CMPLTD_DATE = S.PRINT_CMPLTD_DATE,
    T.REPEAT_RULE = S.REPEAT_RULE,
    T.REPEAT_GRD_CK = S.REPEAT_GRD_CK,
    T.CANCEL_REASON = S.CANCEL_REASON,
    T.WD_WO_PEN_REASON = S.WD_WO_PEN_REASON,
    T.WD_W_PEN_GRD_BAS = S.WD_W_PEN_GRD_BAS,
    T.WD_W_PEN_GRADE = S.WD_W_PEN_GRADE,
    T.WD_W_PEN2_GRADE = S.WD_W_PEN2_GRADE,
    T.WD_W_PEN2_GRD_BAS = S.WD_W_PEN2_GRD_BAS,
    T.DROP_RET_RSN = S.DROP_RET_RSN,
    T.DROP_PEN_GRADE = S.DROP_PEN_GRADE,
    T.DROP_PEN_GRADE_2 = S.DROP_PEN_GRADE_2,
    T.DROP_PEN_GRD_BAS = S.DROP_PEN_GRD_BAS,
    T.DROP_PEN_GRD_BAS_2 = S.DROP_PEN_GRD_BAS_2,
    T.OEE_IND = S.OEE_IND,
    T.REPEAT_ENRL_CTL = S.REPEAT_ENRL_CTL,
    T.REPEAT_ENRL_SUSP = S.REPEAT_ENRL_SUSP,
    T.REPEAT_GRD_SUSP = S.REPEAT_GRD_SUSP,
    T.REPEAT_CRSE_ERROR = S.REPEAT_CRSE_ERROR,
    T.SSR_LAST_PRS_DT = S.SSR_LAST_PRS_DT,
    T.SSR_LAST_ADM_TERM = S.SSR_LAST_ADM_TERM,
    T.DATA_ORIGIN = 'S',
    T.LASTUPD_EW_DTTM = sysdate,
    T.BATCH_SID = 1234
where 
    T.EFF_STATUS <> S.EFF_STATUS or 
    T.DESCR <> S.DESCR or 
    T.DESCRSHORT <> S.DESCRSHORT or 
    T.ACAD_CAREER <> S.ACAD_CAREER or 
    T.ACAD_CALENDAR_ID <> S.ACAD_CALENDAR_ID or 
    T.ADVISOR_EDIT <> S.ADVISOR_EDIT or 
    T.LEVEL_LOAD_RULE <> S.LEVEL_LOAD_RULE or 
    T.ACAD_GROUP <> S.ACAD_GROUP or 
    T.ACAD_PLAN <> S.ACAD_PLAN or 
    T.CAMPUS <> S.CAMPUS or 
    T.FIRST_TERM_VALID <> S.FIRST_TERM_VALID or 
    T.CAR_PTR_EXC_RULE <> S.CAR_PTR_EXC_RULE or 
    T.CAR_PTR_EXC_FG <> S.CAR_PTR_EXC_FG or 
    T.FA_PRIMACY_NBR <> S.FA_PRIMACY_NBR or 
    T.FA_ELIGIBILITY <> S.FA_ELIGIBILITY or 
    T.PROG_NORM_COMPLTN <> S.PROG_NORM_COMPLTN or 
    T.RESIDENCY_REQ <> S.RESIDENCY_REQ or 
    T.CIP_CODE <> S.CIP_CODE or 
    T.HEGIS_CODE <> S.HEGIS_CODE or 
    T.CRSE_COUNT_ENRL <> S.CRSE_COUNT_ENRL or 
    T.CRSE_COUNT_MIN <> S.CRSE_COUNT_MIN or 
    T.ACAD_ORG <> S.ACAD_ORG or 
    T.SPLIT_OWNER <> S.SPLIT_OWNER or 
    T.ACAD_PROG_DUAL <> S.ACAD_PROG_DUAL or 
    T.GRADING_SCHEME <> S.GRADING_SCHEME or 
    T.GRADING_BASIS <> S.GRADING_BASIS or 
    T.GRADE_TRANSFER <> S.GRADE_TRANSFER or 
    T.TRANSCRIPT_LEVEL <> S.TRANSCRIPT_LEVEL or 
    T.ACAD_STDNG_RULE <> S.ACAD_STDNG_RULE or 
    T.ASSOC_PROG_AS <> S.ASSOC_PROG_AS or 
    T.CALC_AS_BATCH_ONLY <> S.CALC_AS_BATCH_ONLY or 
    T.OBEY_FULLY_GRD_AS <> S.OBEY_FULLY_GRD_AS or 
    T.EXCL_TRM_CAT_AS_1 <> S.EXCL_TRM_CAT_AS_1 or 
    T.EXCL_TRM_CAT_AS_2 <> S.EXCL_TRM_CAT_AS_2 or 
    T.EXCL_TRM_CAT_AS_3 <> S.EXCL_TRM_CAT_AS_3 or 
    T.HONOR_AWARD_RULE <> S.HONOR_AWARD_RULE or 
    T.ASSOC_PROG_HA <> S.ASSOC_PROG_HA or 
    T.CALC_HA_BATCH_ONLY <> S.CALC_HA_BATCH_ONLY or 
    T.OBEY_FULLY_GRD_HA <> S.OBEY_FULLY_GRD_HA or 
    T.EXCL_TRM_CAT_HA_1 <> S.EXCL_TRM_CAT_HA_1 or 
    T.EXCL_TRM_CAT_HA_2 <> S.EXCL_TRM_CAT_HA_2 or 
    T.EXCL_TRM_CAT_HA_3 <> S.EXCL_TRM_CAT_HA_3 or 
    T.HONOR_DT_FG <> S.HONOR_DT_FG or 
    T.INCOMPLETE_GRADE <> S.INCOMPLETE_GRADE or 
    T.LAPSE_GRADE <> S.LAPSE_GRADE or 
    T.LAPSE_TO_GRADE <> S.LAPSE_TO_GRADE or 
    T.LAPSE_DAYS <> S.LAPSE_DAYS or 
    T.LAPSE_NOTE_ID <> S.LAPSE_NOTE_ID or 
    T.PRINT_LAPSE_DATE <> S.PRINT_LAPSE_DATE or 
    T.CMPLTD_NOTE_ID <> S.CMPLTD_NOTE_ID or 
    T.PRINT_CMPLTD_DATE <> S.PRINT_CMPLTD_DATE or 
    T.REPEAT_RULE <> S.REPEAT_RULE or 
    T.REPEAT_GRD_CK <> S.REPEAT_GRD_CK or 
    T.CANCEL_REASON <> S.CANCEL_REASON or 
    T.WD_WO_PEN_REASON <> S.WD_WO_PEN_REASON or 
    T.WD_W_PEN_GRD_BAS <> S.WD_W_PEN_GRD_BAS or 
    T.WD_W_PEN_GRADE <> S.WD_W_PEN_GRADE or 
    T.WD_W_PEN2_GRADE <> S.WD_W_PEN2_GRADE or 
    T.WD_W_PEN2_GRD_BAS <> S.WD_W_PEN2_GRD_BAS or 
    T.DROP_RET_RSN <> S.DROP_RET_RSN or 
    T.DROP_PEN_GRADE <> S.DROP_PEN_GRADE or 
    T.DROP_PEN_GRADE_2 <> S.DROP_PEN_GRADE_2 or 
    T.DROP_PEN_GRD_BAS <> S.DROP_PEN_GRD_BAS or 
    T.DROP_PEN_GRD_BAS_2 <> S.DROP_PEN_GRD_BAS_2 or 
    T.OEE_IND <> S.OEE_IND or 
    T.REPEAT_ENRL_CTL <> S.REPEAT_ENRL_CTL or 
    T.REPEAT_ENRL_SUSP <> S.REPEAT_ENRL_SUSP or 
    T.REPEAT_GRD_SUSP <> S.REPEAT_GRD_SUSP or 
    T.REPEAT_CRSE_ERROR <> S.REPEAT_CRSE_ERROR or 
    nvl(trim(T.SSR_LAST_PRS_DT),0) <> nvl(trim(S.SSR_LAST_PRS_DT),0) or 
    T.SSR_LAST_ADM_TERM <> S.SSR_LAST_ADM_TERM or 
    T.DATA_ORIGIN = 'D' 
when not matched then 
insert (
    T.INSTITUTION,
    T.ACAD_PROG,
    T.EFFDT,
    T.SRC_SYS_ID, 
    T.EFF_STATUS, 
    T.DESCR,
    T.DESCRSHORT, 
    T.ACAD_CAREER,
    T.ACAD_CALENDAR_ID, 
    T.ADVISOR_EDIT, 
    T.LEVEL_LOAD_RULE,
    T.ACAD_GROUP, 
    T.ACAD_PLAN,
    T.CAMPUS, 
    T.FIRST_TERM_VALID, 
    T.CAR_PTR_EXC_RULE, 
    T.CAR_PTR_EXC_FG, 
    T.FA_PRIMACY_NBR, 
    T.FA_ELIGIBILITY, 
    T.PROG_NORM_COMPLTN,
    T.RESIDENCY_REQ,
    T.CIP_CODE, 
    T.HEGIS_CODE, 
    T.CRSE_COUNT_ENRL,
    T.CRSE_COUNT_MIN, 
    T.ACAD_ORG, 
    T.SPLIT_OWNER,
    T.ACAD_PROG_DUAL, 
    T.GRADING_SCHEME, 
    T.GRADING_BASIS,
    T.GRADE_TRANSFER, 
    T.TRANSCRIPT_LEVEL, 
    T.ACAD_STDNG_RULE,
    T.ASSOC_PROG_AS,
    T.CALC_AS_BATCH_ONLY, 
    T.OBEY_FULLY_GRD_AS,
    T.EXCL_TRM_CAT_AS_1,
    T.EXCL_TRM_CAT_AS_2,
    T.EXCL_TRM_CAT_AS_3,
    T.HONOR_AWARD_RULE, 
    T.ASSOC_PROG_HA,
    T.CALC_HA_BATCH_ONLY, 
    T.OBEY_FULLY_GRD_HA,
    T.EXCL_TRM_CAT_HA_1,
    T.EXCL_TRM_CAT_HA_2,
    T.EXCL_TRM_CAT_HA_3,
    T.HONOR_DT_FG,
    T.INCOMPLETE_GRADE, 
    T.LAPSE_GRADE,
    T.LAPSE_TO_GRADE, 
    T.LAPSE_DAYS, 
    T.LAPSE_NOTE_ID,
    T.PRINT_LAPSE_DATE, 
    T.CMPLTD_NOTE_ID, 
    T.PRINT_CMPLTD_DATE,
    T.REPEAT_RULE,
    T.REPEAT_GRD_CK,
    T.CANCEL_REASON,
    T.WD_WO_PEN_REASON, 
    T.WD_W_PEN_GRD_BAS, 
    T.WD_W_PEN_GRADE, 
    T.WD_W_PEN2_GRADE,
    T.WD_W_PEN2_GRD_BAS,
    T.DROP_RET_RSN, 
    T.DROP_PEN_GRADE, 
    T.DROP_PEN_GRADE_2, 
    T.DROP_PEN_GRD_BAS, 
    T.DROP_PEN_GRD_BAS_2, 
    T.OEE_IND,
    T.REPEAT_ENRL_CTL,
    T.REPEAT_ENRL_SUSP, 
    T.REPEAT_GRD_SUSP,
    T.REPEAT_CRSE_ERROR,
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
    S.ACAD_PROG,
    S.EFFDT,
    'CS90', 
    S.EFF_STATUS, 
    S.DESCR,
    S.DESCRSHORT, 
    S.ACAD_CAREER,
    S.ACAD_CALENDAR_ID, 
    S.ADVISOR_EDIT, 
    S.LEVEL_LOAD_RULE,
    S.ACAD_GROUP, 
    S.ACAD_PLAN,
    S.CAMPUS, 
    S.FIRST_TERM_VALID, 
    S.CAR_PTR_EXC_RULE, 
    S.CAR_PTR_EXC_FG, 
    S.FA_PRIMACY_NBR, 
    S.FA_ELIGIBILITY, 
    S.PROG_NORM_COMPLTN,
    S.RESIDENCY_REQ,
    S.CIP_CODE, 
    S.HEGIS_CODE, 
    S.CRSE_COUNT_ENRL,
    S.CRSE_COUNT_MIN, 
    S.ACAD_ORG, 
    S.SPLIT_OWNER,
    S.ACAD_PROG_DUAL, 
    S.GRADING_SCHEME, 
    S.GRADING_BASIS,
    S.GRADE_TRANSFER, 
    S.TRANSCRIPT_LEVEL, 
    S.ACAD_STDNG_RULE,
    S.ASSOC_PROG_AS,
    S.CALC_AS_BATCH_ONLY, 
    S.OBEY_FULLY_GRD_AS,
    S.EXCL_TRM_CAT_AS_1,
    S.EXCL_TRM_CAT_AS_2,
    S.EXCL_TRM_CAT_AS_3,
    S.HONOR_AWARD_RULE, 
    S.ASSOC_PROG_HA,
    S.CALC_HA_BATCH_ONLY, 
    S.OBEY_FULLY_GRD_HA,
    S.EXCL_TRM_CAT_HA_1,
    S.EXCL_TRM_CAT_HA_2,
    S.EXCL_TRM_CAT_HA_3,
    S.HONOR_DT_FG,
    S.INCOMPLETE_GRADE, 
    S.LAPSE_GRADE,
    S.LAPSE_TO_GRADE, 
    S.LAPSE_DAYS, 
    S.LAPSE_NOTE_ID,
    S.PRINT_LAPSE_DATE, 
    S.CMPLTD_NOTE_ID, 
    S.PRINT_CMPLTD_DATE,
    S.REPEAT_RULE,
    S.REPEAT_GRD_CK,
    S.CANCEL_REASON,
    S.WD_WO_PEN_REASON, 
    S.WD_W_PEN_GRD_BAS, 
    S.WD_W_PEN_GRADE, 
    S.WD_W_PEN2_GRADE,
    S.WD_W_PEN2_GRD_BAS,
    S.DROP_RET_RSN, 
    S.DROP_PEN_GRADE, 
    S.DROP_PEN_GRADE_2, 
    S.DROP_PEN_GRD_BAS, 
    S.DROP_PEN_GRD_BAS_2, 
    S.OEE_IND,
    S.REPEAT_ENRL_CTL,
    S.REPEAT_ENRL_SUSP, 
    S.REPEAT_GRD_SUSP,
    S.REPEAT_CRSE_ERROR,
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

strMessage01    := '# of PS_ACAD_PROG_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_PROG_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_ACAD_PROG_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_ACAD_PROG_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_ACAD_PROG_TBL';
update AMSTG_OWNER.PS_ACAD_PROG_TBL T
   set T.DATA_ORIGIN = 'D',
          T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select INSTITUTION, ACAD_PROG, EFFDT
   from AMSTG_OWNER.PS_ACAD_PROG_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_PROG_TBL') = 'Y'
  minus
 select INSTITUTION, ACAD_PROG, EFFDT
   from SYSADM.PS_ACAD_PROG_TBL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_ACAD_PROG_TBL') = 'Y'

   ) S
 where T.INSTITUTION = S.INSTITUTION
   and T.ACAD_PROG = S.ACAD_PROG
   and T.EFFDT = S.EFFDT
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_ACAD_PROG_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_ACAD_PROG_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_ACAD_PROG_TBL'
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

END AM_PS_ACAD_PROG_TBL_P;
/
