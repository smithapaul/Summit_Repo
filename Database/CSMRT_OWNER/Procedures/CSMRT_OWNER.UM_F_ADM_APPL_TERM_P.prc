DROP PROCEDURE CSMRT_OWNER.UM_F_ADM_APPL_TERM_P
/

--
-- UM_F_ADM_APPL_TERM_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_ADM_APPL_TERM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_ADM_APPL_TERM
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_ADM_APPL_TERM';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_ADM_APPL_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_ADM_APPL_TERM';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_ADM_APPL_TERM');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_TERM disable constraint PK_UM_F_ADM_APPL_TERM';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_ADM_APPL_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_ADM_APPL_TERM';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_ADM_APPL_TERM
      WITH E
        AS (  SELECT TERM_SID,
                     PERSON_SID,
                     SRC_SYS_ID,
                     SUM(ENROLL_CNT) ENROLL_CNT,        -- Added Nov 2015
                     SUM (CE_CREDITS) TOT_CE_CREDITS,
                     SUM (DAY_CREDITS) TOT_DAY_CREDITS
                FROM UM_F_CLASS_ENRLMT
            GROUP BY TERM_SID, PERSON_SID, SRC_SYS_ID)
   SELECT /*+ INLINE PARALLEL(8) */
          A.ADM_APPL_SID,
          NVL (T.TERM_SID, A.ADMIT_TERM_SID) TERM_SID,
          NVL (T.PERSON_SID, A.APPLCNT_SID) PERSON_SID,
          NVL (T.SRC_SYS_ID, A.SRC_SYS_ID) SRC_SYS_ID,
          T.INSTITUTION_CD,
          T.ACAD_CAR_CD,
          T.TERM_CD,
          T.PERSON_ID,
          NVL (ACAD_GRP_ADVIS_SID, 2147483646) ACAD_GRP_ADVIS_SID,
          NVL (ACAD_LOAD_APPR_SID, 2147483646) ACAD_LOAD_APPR_SID,
          NVL (T.ACAD_LOAD_SID, 2147483646) ACAD_LOAD_SID,
          NVL (STRT_ACAD_LVL_SID, 2147483646) STRT_ACAD_LVL_SID,
          NVL (END_ACAD_LVL_SID, 2147483646) END_ACAD_LVL_SID,
          NVL (PRJTD_ACAD_LVL_SID, 2147483646) PRJTD_ACAD_LVL_SID,
          NVL (PRI_ACAD_PROG_SID, 2147483646) PRI_ACAD_PROG_SID,
          NVL (ACAD_STNDNG_SID, 2147483646) ACAD_STNDNG_SID,
          NVL (BILL_CAR_SID, 2147483646) BILL_CAR_SID,
          NVL (FA_LOAD_SID, 2147483646) FA_LOAD_SID,
          ACAD_CAR_FIRST_FLG,
          ACAD_LOAD_DT,
          ACAD_YR_SID,
          CLASS_RANK_NUM,
          CLASS_RANK_TOT,
          COUNTRY,
          ELIG_TO_ENROLL_FLG,
          ENRL_ON_TRN_DT,
          EXT_ORG_ID,
          FA_ELIG_FLG,
          FA_STATS_CALC_REQ_FLG,
          FA_STATS_CALC_DTTM,
          FORM_OF_STUDY,
          FORM_OF_STUDY_SD,
          FORM_OF_STUDY_LD,
          FULLY_ENRL_DT,
          FULLY_GRADED_DT,
          LAST_ATTND_DT,
          LOCK_IN_AMT,
          LOCK_IN_DT,
          MAX_CRSE_CNT,
          NSLDS_LOAN_YEAR,
          NSLDS_LOAN_YEAR_SD,
          NSLDS_LOAN_YEAR_LD,
          OVRD_ACAD_LVL_PROJ_FLG,
          OVRD_ACAD_LVL_ALL_FLG,
          OVRD_BILL_UNITS_FLG,
          OVRD_INIT_ADD_FEE_FLG,
          OVRD_INIT_ENR_FEE_FLG,
          OVRD_MAX_UNITS_FLG,
          OVRD_TUIT_GROUP,
          OVRD_WDRW_SCHED,
          PRJTD_BILL_UNIT,
          PRO_RATA_ELIG_FLG,
          REFUND_PCT,
          REFUND_SCHEME,
          REG_CARD_DT,
          REG_FLG,
          RESET_CUM_STATS_FLG,
          SEL_GROUP,
          SSR_ACTV_DT,
          STATS_ON_TRN_DT,
          T.STDNT_CAR_NUM,
          STUDY_AGREEMENT,
          TERM_TYPE,
          TUIT_CALC_REQ_FLG,
          TUIT_CALC_DTTM,
          UNTPRG_CHG_NSLC_DT,
          UNIT_MULTIPLIER,
          WDN_DT,
          WITHDRAW_CODE,
          WITHDRAW_CODE_SD,
          WITHDRAW_CODE_LD,
          WITHDRAW_REASON,
          WITHDRAW_REASON_SD,
          WITHDRAW_REASON_LD,
          NVL (E.ENROLL_CNT, 0) ENROLL_CNT,         -- Added Nov 2015
          NVL (E.TOT_CE_CREDITS, 0) TOT_CE_CREDITS,
          NVL (E.TOT_DAY_CREDITS, 0) TOT_DAY_CREDITS,
          (NVL (E.TOT_CE_CREDITS, 0) + NVL (E.TOT_DAY_CREDITS, 0)) TOT_CREDITS,
          UNIT_TAKEN_GPA,
          UNIT_TAKEN_NOGPA,
          GRADE_PTS,
          CUR_GPA,
          UNIT_PASSED_GPA,
          UNIT_PASSED_NOGPA,
          UNIT_INPROG_GPA,
          UNIT_INPROG_NOGPA,
          UNIT_TAKEN_PROGRESS,
          UNIT_PASSED_PROGRESS,
          UNIT_AUDIT,
          TRF_UNIT_TAKEN_GPA,
          TRF_UNIT_TAKEN_NOGPA,
          TRF_GRADE_PTS,
          TRF_CUR_GPA,
          TRF_UNIT_PASSED_GPA,
          TRF_UNIT_PASSED_NOGPA,
          (TRF_UNIT_TAKEN_GPA + TRF_UNIT_TAKEN_NOGPA) TRF_UNIT_TOT_GRADED,
          (TRF_UNIT_TRANSFER + TRF_UNIT_TEST_CREDIT + TRF_UNIT_OTHER)
             TRF_UNIT_TOT,
          TRF_UNIT_ADJUST,
          (  (  TRF_UNIT_TAKEN_GPA
              + TRF_UNIT_TAKEN_NOGPA
              + TRF_UNIT_TRANSFER
              + TRF_UNIT_TEST_CREDIT
              + TRF_UNIT_OTHER)
           - TRF_UNIT_ADJUST)
             TRF_UNIT_TOT_ADJUSTED,
          TRF_UNIT_TEST_CREDIT,
          TRF_UNIT_TRANSFER,
          TRF_UNIT_OTHER,
          (UNIT_TAKEN_GPA + TRF_UNIT_TAKEN_GPA) COMB_UNIT_TAKEN_GPA,
          (UNIT_TAKEN_NOGPA + TRF_UNIT_TAKEN_NOGPA) COMB_UNIT_TAKEN_NOGPA,
          (GRADE_PTS + TRF_GRADE_PTS) COMB_GRADE_PTS,
          COMB_CUR_GPA,
          (UNIT_PASSED_GPA + TRF_UNIT_PASSED_GPA) COMB_UNIT_PASSED_GPA,
          (UNIT_PASSED_NOGPA + TRF_UNIT_PASSED_NOGPA) COMB_UNIT_PASSED_NOGPA,
          (  UNIT_PASSED_GPA
           + TRF_UNIT_PASSED_GPA
           + UNIT_PASSED_NOGPA
           + TRF_UNIT_PASSED_NOGPA)
             COMB_UNIT_PASSED,
          COMB_UNIT_TOT,
          CUM_UNIT_TAKEN_GPA,
          CUM_UNIT_TAKEN_NOGPA,
          CUM_GRADE_PTS,
          CUM_CUR_GPA,
          CUM_UNIT_PASSED_GPA,
          CUM_UNIT_PASSED_NOGPA,
          CUM_UNIT_INPROG_GPA,
          CUM_UNIT_INPROG_NOGPA,
          CUM_UNIT_TAKEN_PROGRESS,
          CUM_UNIT_PASSED_PROGRESS,
          CUM_UNIT_AUDIT,
          CUM_TRF_UNIT_TAKEN_GPA,
          CUM_TRF_UNIT_TAKEN_NOGPA,
          CUM_TRF_GRADE_PTS,
          CUM_TRF_CUR_GPA,
          CUM_TRF_UNIT_PASSED_GPA,
          CUM_TRF_UNIT_PASSED_NOGPA,
          (CUM_TRF_UNIT_TAKEN_GPA + CUM_TRF_UNIT_TAKEN_NOGPA)
             CUM_TRF_UNIT_TOT_GRADED,
          (  CUM_TRF_UNIT_TRANSFER
           + CUM_TRF_UNIT_TEST_CREDIT
           + CUM_TRF_UNIT_OTHER)
             CUM_TRF_UNIT_TOT,
          CUM_TRF_UNIT_ADJUST,
          (  (  CUM_TRF_UNIT_TAKEN_GPA
              + CUM_TRF_UNIT_TAKEN_NOGPA
              + CUM_TRF_UNIT_TRANSFER
              + CUM_TRF_UNIT_TEST_CREDIT
              + CUM_TRF_UNIT_OTHER)
           - CUM_TRF_UNIT_ADJUST)
             CUM_TRF_UNIT_TOT_ADJUSTED,
          CUM_TRF_UNIT_TEST_CREDIT,
          CUM_TRF_UNIT_TRANSFER,
          CUM_TRF_UNIT_OTHER,
          CUM_COMB_UNIT_TAKEN_GPA,
          CUM_COMB_UNIT_TAKEN_NOGPA,
          CUM_COMB_GRADE_PTS,
          CUM_COMB_CUR_GPA,
          CUM_COMB_UNIT_PASSED_GPA,
          CUM_COMB_UNIT_PASSED_NOGPA,
          CUM_COMB_UNIT_PASSED,
          CUM_COMB_UNIT_TOT
     FROM UM_F_ADM_APPL_STAT A
          JOIN UM_F_ADM_APPL_ENRL AE ON A.ADM_APPL_SID = AE.ADM_APPL_SID
          LEFT OUTER JOIN UM_F_STDNT_TERM T
             ON     A.APPLCNT_SID = T.PERSON_SID
                --      AND A.ADMIT_TERM_SID = T.TERM_SID
                AND (CASE
                        WHEN NOT (    AE.INSTITUTION_CD = 'UMLOW'
                                  AND AE.ACAD_CAR_CD IN ('CSCE', 'GRAD')
                                  AND AE.ENROLL_CNT = 0)
                        THEN
                           A.ADMIT_TERM_SID
                        WHEN     SUBSTR (AE.ADMIT_TERM_CD, -2, 2) = '10'
                             AND AE.PREV_TERM_SID IS NOT NULL
                             AND AE.PREV_ENROLL_CNT > 0
                        THEN
                           AE.PREV_TERM_SID
                        WHEN     SUBSTR (AE.ADMIT_TERM_CD, -2, 2) IN ('40',
                                                                      '50')
                             AND AE.NEXT_TERM_SID IS NOT NULL
                             AND AE.NEXT_ENROLL_CNT > 0
                        THEN
                           AE.NEXT_TERM_SID
                        ELSE
                           A.ADMIT_TERM_SID
                     END) = T.TERM_SID
                AND A.SRC_SYS_ID = T.SRC_SYS_ID
--                AND A.STU_CAR_NBR_SR = T.STDNT_CAR_NUM1 -- Is this the right STDNT_CAR_NUM???     -- Old!!!!!!!!!!!!!!!!!!!!
--                AND A.STU_CAR_NBR_SR = T.PS_STDNT_CAR_NUM -- Is this the right STDNT_CAR_NUM???   -- Removed Oct 2017
          LEFT OUTER JOIN E
             ON     E.TERM_SID = T.TERM_SID
                AND E.PERSON_SID = T.PERSON_SID
                AND E.SRC_SYS_ID = T.SRC_SYS_ID;
strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_ADM_APPL_TERM rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_ADM_APPL_TERM',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_ADM_APPL_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_ADM_APPL_TERM enable constraint PK_UM_F_ADM_APPL_TERM';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_ADM_APPL_TERM');

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

END UM_F_ADM_APPL_TERM_P;
/
