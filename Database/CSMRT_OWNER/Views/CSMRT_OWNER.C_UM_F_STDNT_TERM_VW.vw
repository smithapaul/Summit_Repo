CREATE OR REPLACE VIEW C_UM_F_STDNT_TERM_VW
BEQUEATH DEFINER
AS 
SELECT TERM_SID,
           PERSON_SID,
           SRC_SYS_ID,
           INSTITUTION_CD,
           ACAD_CAR_CD,
           TERM_CD,
           PERSON_ID,
           INSTITUTION_SID,
           ACAD_CAR_SID,
           STDNT_TERM_KEY,
           PS_STDNT_CAR_NUM,
           PS_PROG_SID,
           EFFDT,
           PS_PROG_STAT_SID,
           PS_PROG_ACN_SID,
           PS_PROG_ACN_RSN_SID,
           PS_ADMIT_TERM_SID,
           PS_EXP_GRAD_TERM_SID,
           PS_DEGR_CHKOUT_LAST_EGT,
           PS_DEGR_CHKOUT_LAST_EGT_LD,
           PS_DEGR_CHKOUT_LAST_EGT_EFFDT,
           MAJ1_ACAD_PLAN_SID,
           MAJ1_PLAN_SEQUENCE,
           MAJ1_SPLAN1_SID,
           MAJ1_SPLAN2_SID,
           MAJ1_SPLAN3_SID,
           MAJ1_SPLAN4_SID,
           MAJ2_ACAD_PLAN_SID,
           MAJ2_SPLAN1_SID,
           MAJ3_ACAD_PLAN_SID,
           MAJ3_SPLAN1_SID,
           MAJ4_ACAD_PLAN_SID,
           MIN1_ACAD_PLAN_SID,
           MIN2_ACAD_PLAN_SID,
           MIN3_ACAD_PLAN_SID,
           MIN4_ACAD_PLAN_SID,
           OTH1_ACAD_PLAN_SID,
           OTH2_ACAD_PLAN_SID,
           ACAD_GRP_ADVIS_SID,
           ACAD_LOAD_APPR_SID,
           ACAD_LOAD_SID,
           STRT_ACAD_LVL_SID,
           END_ACAD_LVL_SID,
           PRJTD_ACAD_LVL_SID,
           PRI_ACAD_PROG_SID,
           ACAD_STNDNG_SID,
           ACAD_STNDNG_TERM_CD_DESC,                               -- May 2018
           TERM_ACAD_STNDNG_SID,                                   -- May 2018
           BILL_CAR_SID,
           FA_LOAD_SID,
           PERSON_ATHL_SID,
           PERSON_SRVC_IND_SID,
           ACAD_CAR_FIRST_FLG,
           ACAD_LOAD_DT,
           ACAD_YR_SID,
           ADMIT_TERM_CD,
           ADMIT_TERM_SD,
           ADMIT_TERM_CD_DESC,
           ADMIT_TYPE_LD,
           CE_ADMIT_FLG,
           CLASS_RANK_NUM,
           CLASS_RANK_TOT,
           COUNTRY,
           ELIG_TO_ENROLL_FLG,
           ENRL_ON_TRN_DT,
           EXT_ORG_ID,
           FA_APPL_FLG,
           FA_ELIG_FLG,
           FA_PELL_ELIGIBILITY,                                    -- Apr 2018
           FA_PELL_DISB_FLAG,                                      -- Apr 2018
           FA_LOAN_DISB_FLAG,                                      -- Apr 2018
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
           STACK_BEGIN_FLG,                                        -- Mar 2018
           STACK_CONTINUE_FLG,                                     -- Mar 2018
           STACK_READMIT_FLG,                                      -- Mar 2018
           STATS_ON_TRN_DT,
           STDNT_CAR_NUM,
           STUDY_AGREEMENT,
           TERM_ACTV_FLG,
           TERM_ACTV_MAX_TERM_CD,
           TERM_ACTV_MAX_TERM_SID,                                -- July 2016
           TERM_BEGIN_DT,                                          -- Mar 2018
           TERM_END_DT,                                            -- Mar 2018
           TERM_TYPE,
           TUIT_CALC_REQ_FLG,
           TUIT_CALC_DTTM,
           UGRD_SECOND_DEGR_FLG,
           UMBOS_HON_FLG,
           UNTPRG_CHG_NSLC_DT,
           UNIT_MULTIPLIER,
           WDN_DT,
           WITHDRAW_CODE,
           WITHDRAW_CODE_SD,
           WITHDRAW_CODE_LD,
           WITHDRAW_REASON,
           WITHDRAW_REASON_SD,
           WITHDRAW_REASON_LD,
           UNDUP_STDNT_CNT,
           TERM_ENRL_ORDER,                                       -- July 2016
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
           TRF_UNIT_TOT_GRADED,
           TRF_UNIT_TOT,
           TRF_UNIT_ADJUST,
           TRF_UNIT_TOT_ADJUSTED,
           TRF_UNIT_TEST_CREDIT,
           TRF_UNIT_TRANSFER,
           TRF_UNIT_OTHER,
           COMB_UNIT_TAKEN_GPA,
           COMB_UNIT_TAKEN_NOGPA,
           COMB_GRADE_PTS,
           COMB_CUR_GPA,
           COMB_UNIT_PASSED_GPA,
           COMB_UNIT_PASSED_NOGPA,
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
           CUM_TRF_UNIT_TOT_GRADED,
           CUM_TRF_UNIT_TOT,
           CUM_TRF_UNIT_ADJUST,
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
           MAX_UNIT_AUDIT,                                         -- SMT-8215
           MAX_UNIT_NOGPA,                                         -- SMT-8215
           MAX_UNIT_TOT,                                           -- SMT-8215
           MAX_UNIT_WAIT,                                          -- SMT-8215
           CUM_COMB_UNIT_TOT,
           ENRLMT_MAX_TERM_CD,
           ENRLMT_MAX_TERM_SID,                                    -- Mar 2018
           ENRLMT_MIN_TERM_CD,                                    -- June 2018
           ENRLMT_MIN_TERM_SID,                                   -- June 2018
           ENRLMT_MIN_PERSON_TERM_CD,                             -- June 2018
           ENRLMT_MIN_PERSON_TERM_SID,                            -- June 2018
           ENRLMT_PREV_TERM_CD,                                   -- June 2018
           ENRLMT_PREV_TERM_SID,                                  -- June 2018
           ENRL_ADD_MAX_DT,
           ENRL_DROP_MAX_DT,
           AUDIT_CNT,
           cast(AUDIT_ONLY_FLG as varchar2(1)) AS AUDIT_ONLY_FLG,
           BILLING_UNIT,
           CE_CREDITS,
           CE_FTE,
           cast(CE_ONLY_FLG as varchar2(1)) AS CE_ONLY_FLG,
           DAY_CREDITS,
           DAY_FTE,
           cast(DAY_ONLY_FLG as varchar2(1)) AS DAY_ONLY_FLG,
           CRSE_CNT,
           DROP_CNT,
           ENROLL_CNT,
           ENROLL_DT,
           cast(ENROLL_FLG as varchar2(1)) AS ENROLL_FLG,
           ERN_UNIT,
           IFTE_CNT,
           ONLINE_CNT,
           ONLINE_CREDITS,
           CE_ONLINE_CREDITS,                              -- Added APRIL 2021
           CE_OTHER_CREDITS,                               -- Added APRIL 2021
           cast(ONLINE_ONLY_FLG as varchar2(1)) AS ONLINE_ONLY_FLG,
           PRGRS_UNIT,
           PRGRS_FA_UNIT,
           TAKEN_UNIT,
           TOT_CREDITS,
           DAY_ONLINE_CREDITS,                             -- Added APRIL 2021
           DAY_OTHER_CREDITS,                              -- Added APRIL 2021
           TOT_FTE,
           WAIT_CNT,
           TERM_COUNT
      FROM (  SELECT UM_F_STDNT_TERM.TERM_SID
                         TERM_SID,
                     UM_F_STDNT_TERM.PERSON_SID
                         PERSON_SID,
                     UM_F_STDNT_TERM.SRC_SYS_ID
                         SRC_SYS_ID,
                     UM_F_STDNT_TERM.INSTITUTION_CD
                         INSTITUTION_CD,
                     UM_F_STDNT_TERM.ACAD_CAR_CD
                         ACAD_CAR_CD,
                     UM_F_STDNT_TERM.TERM_CD
                         TERM_CD,
                     UM_F_STDNT_TERM.PERSON_ID
                         PERSON_ID,
                     UM_F_STDNT_TERM.INSTITUTION_SID
                         INSTITUTION_SID,
                     UM_F_STDNT_TERM.ACAD_CAR_SID
                         ACAD_CAR_SID,
                     UM_F_STDNT_TERM.STDNT_TERM_KEY
                         STDNT_TERM_KEY,
                     UM_F_STDNT_TERM.PS_STDNT_CAR_NUM
                         PS_STDNT_CAR_NUM,
                     UM_F_STDNT_TERM.PS_PROG_SID
                         PS_PROG_SID,
                     UM_F_STDNT_TERM.PS_EFFDT
                         EFFDT,
                     UM_F_STDNT_TERM.PS_PROG_STAT_SID
                         PS_PROG_STAT_SID,
                     UM_F_STDNT_TERM.PS_PROG_ACN_SID
                         PS_PROG_ACN_SID,
                     UM_F_STDNT_TERM.PS_PROG_ACN_RSN_SID
                         PS_PROG_ACN_RSN_SID,
                     UM_F_STDNT_TERM.PS_ADMIT_TERM_SID
                         PS_ADMIT_TERM_SID,
                     UM_F_STDNT_TERM.PS_EXP_GRAD_TERM_SID
                         PS_EXP_GRAD_TERM_SID,
                     UM_F_STDNT_TERM.PS_DEGR_CHKOUT_LAST_EGT
                         PS_DEGR_CHKOUT_LAST_EGT,
                     UM_F_STDNT_TERM.PS_DEGR_CHKOUT_LAST_EGT_LD
                         PS_DEGR_CHKOUT_LAST_EGT_LD,
                     UM_F_STDNT_TERM.PS_DEGR_CHKOUT_LAST_EGT_EFFDT
                         PS_DEGR_CHKOUT_LAST_EGT_EFFDT,
                     UM_F_STDNT_TERM.MAJ1_ACAD_PLAN_SID
                         MAJ1_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MAJ1_PLAN_SEQUENCE
                         MAJ1_PLAN_SEQUENCE,
                     UM_F_STDNT_TERM.MAJ1_SPLAN1_SID
                         MAJ1_SPLAN1_SID,
                     UM_F_STDNT_TERM.MAJ1_SPLAN2_SID
                         MAJ1_SPLAN2_SID,
                     UM_F_STDNT_TERM.MAJ1_SPLAN3_SID
                         MAJ1_SPLAN3_SID,
                     UM_F_STDNT_TERM.MAJ1_SPLAN4_SID
                         MAJ1_SPLAN4_SID,
                     UM_F_STDNT_TERM.MAJ2_ACAD_PLAN_SID
                         MAJ2_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MAJ2_SPLAN1_SID
                         MAJ2_SPLAN1_SID,
                     UM_F_STDNT_TERM.MAJ3_ACAD_PLAN_SID
                         MAJ3_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MAJ3_SPLAN1_SID
                         MAJ3_SPLAN1_SID,
                     UM_F_STDNT_TERM.MAJ4_ACAD_PLAN_SID
                         MAJ4_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MIN1_ACAD_PLAN_SID
                         MIN1_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MIN2_ACAD_PLAN_SID
                         MIN2_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MIN3_ACAD_PLAN_SID
                         MIN3_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MIN4_ACAD_PLAN_SID
                         MIN4_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.OTH1_ACAD_PLAN_SID
                         OTH1_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.OTH2_ACAD_PLAN_SID
                         OTH2_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.ACAD_GRP_ADVIS_SID
                         ACAD_GRP_ADVIS_SID,
                     UM_F_STDNT_TERM.ACAD_LOAD_APPR_SID
                         ACAD_LOAD_APPR_SID,
                     UM_F_STDNT_TERM.ACAD_LOAD_SID
                         ACAD_LOAD_SID,
                     UM_F_STDNT_TERM.STRT_ACAD_LVL_SID
                         STRT_ACAD_LVL_SID,
                     UM_F_STDNT_TERM.END_ACAD_LVL_SID
                         END_ACAD_LVL_SID,
                     UM_F_STDNT_TERM.PRJTD_ACAD_LVL_SID
                         PRJTD_ACAD_LVL_SID,
                     UM_F_STDNT_TERM.PRI_ACAD_PROG_SID
                         PRI_ACAD_PROG_SID,
                     UM_F_STDNT_TERM.ACAD_STNDNG_SID
                         ACAD_STNDNG_SID,
                     UM_F_STDNT_TERM.ACAD_STNDNG_TERM_CD_DESC
                         ACAD_STNDNG_TERM_CD_DESC,                 -- May 2018
                     UM_F_STDNT_TERM.TERM_ACAD_STNDNG_SID
                         TERM_ACAD_STNDNG_SID,                     -- May 2018
                     UM_F_STDNT_TERM.BILL_CAR_SID
                         BILL_CAR_SID,
                     UM_F_STDNT_TERM.FA_LOAD_SID
                         FA_LOAD_SID,
                     UM_F_STDNT_TERM.PERSON_ATHL_SID
                         PERSON_ATHL_SID,
                     UM_F_STDNT_TERM.PERSON_SRVC_IND_SID
                         PERSON_SRVC_IND_SID,
                     UM_F_STDNT_TERM.ACAD_CAR_FIRST_FLG
                         ACAD_CAR_FIRST_FLG,
                     UM_F_STDNT_TERM.ACAD_LOAD_DT
                         ACAD_LOAD_DT,
                     UM_F_STDNT_TERM.ACAD_YR_SID
                         ACAD_YR_SID,
                     UM_F_STDNT_TERM.ADMIT_TERM_CD
                         ADMIT_TERM_CD,
                     UM_F_STDNT_TERM.ADMIT_TERM_SD
                         ADMIT_TERM_SD,
                     UM_F_STDNT_TERM.ADMIT_TERM_CD_DESC
                         ADMIT_TERM_CD_DESC,
                     UM_F_STDNT_TERM.ADMIT_TYPE_LD
                         ADMIT_TYPE_LD,
                     UM_F_STDNT_TERM.CE_ADMIT_FLG
                         CE_ADMIT_FLG,
                     UM_F_STDNT_TERM.CLASS_RANK_NUM
                         CLASS_RANK_NUM,
                     UM_F_STDNT_TERM.CLASS_RANK_TOT
                         CLASS_RANK_TOT,
                     UM_F_STDNT_TERM.COUNTRY
                         COUNTRY,
                     UM_F_STDNT_TERM.ELIG_TO_ENROLL_FLG
                         ELIG_TO_ENROLL_FLG,
                     UM_F_STDNT_TERM.ENRL_ON_TRN_DT
                         ENRL_ON_TRN_DT,
                     UM_F_STDNT_TERM.EXT_ORG_ID
                         EXT_ORG_ID,
                     UM_F_STDNT_TERM.FA_APPL_FLG
                         FA_APPL_FLG,
                     UM_F_STDNT_TERM.FA_ELIG_FLG
                         FA_ELIG_FLG,
                     UM_F_STDNT_TERM.FA_PELL_ELIGIBILITY
                         FA_PELL_ELIGIBILITY,
                     UM_F_STDNT_TERM.FA_PELL_DISB_FLAG
                         FA_PELL_DISB_FLAG,
                     UM_F_STDNT_TERM.FA_LOAN_DISB_FLAG
                         FA_LOAN_DISB_FLAG,
                     UM_F_STDNT_TERM.FA_STATS_CALC_REQ_FLG
                         FA_STATS_CALC_REQ_FLG,
                     UM_F_STDNT_TERM.FA_STATS_CALC_DTTM
                         FA_STATS_CALC_DTTM,
                     UM_F_STDNT_TERM.FORM_OF_STUDY
                         FORM_OF_STUDY,
                     UM_F_STDNT_TERM.FORM_OF_STUDY_SD
                         FORM_OF_STUDY_SD,
                     UM_F_STDNT_TERM.FORM_OF_STUDY_LD
                         FORM_OF_STUDY_LD,
                     UM_F_STDNT_TERM.FULLY_ENRL_DT
                         FULLY_ENRL_DT,
                     UM_F_STDNT_TERM.FULLY_GRADED_DT
                         FULLY_GRADED_DT,
                     UM_F_STDNT_TERM.LAST_ATTND_DT
                         LAST_ATTND_DT,
                     UM_F_STDNT_TERM.LOCK_IN_AMT
                         LOCK_IN_AMT,
                     UM_F_STDNT_TERM.LOCK_IN_DT
                         LOCK_IN_DT,
                     UM_F_STDNT_TERM.MAX_CRSE_CNT
                         MAX_CRSE_CNT,
                     UM_F_STDNT_TERM.NSLDS_LOAN_YEAR
                         NSLDS_LOAN_YEAR,
                     UM_F_STDNT_TERM.NSLDS_LOAN_YEAR_SD
                         NSLDS_LOAN_YEAR_SD,
                     UM_F_STDNT_TERM.NSLDS_LOAN_YEAR_LD
                         NSLDS_LOAN_YEAR_LD,
                     UM_F_STDNT_TERM.OVRD_ACAD_LVL_PROJ_FLG
                         OVRD_ACAD_LVL_PROJ_FLG,
                     UM_F_STDNT_TERM.OVRD_ACAD_LVL_ALL_FLG
                         OVRD_ACAD_LVL_ALL_FLG,
                     UM_F_STDNT_TERM.OVRD_BILL_UNITS_FLG
                         OVRD_BILL_UNITS_FLG,
                     UM_F_STDNT_TERM.OVRD_INIT_ADD_FEE_FLG
                         OVRD_INIT_ADD_FEE_FLG,
                     UM_F_STDNT_TERM.OVRD_INIT_ENR_FEE_FLG
                         OVRD_INIT_ENR_FEE_FLG,
                     UM_F_STDNT_TERM.OVRD_MAX_UNITS_FLG
                         OVRD_MAX_UNITS_FLG,
                     UM_F_STDNT_TERM.OVRD_TUIT_GROUP
                         OVRD_TUIT_GROUP,
                     UM_F_STDNT_TERM.OVRD_WDRW_SCHED
                         OVRD_WDRW_SCHED,
                     UM_F_STDNT_TERM.PRJTD_BILL_UNIT
                         PRJTD_BILL_UNIT,
                     UM_F_STDNT_TERM.PRO_RATA_ELIG_FLG
                         PRO_RATA_ELIG_FLG,
                     UM_F_STDNT_TERM.REFUND_PCT
                         REFUND_PCT,
                     UM_F_STDNT_TERM.REFUND_SCHEME
                         REFUND_SCHEME,
                     UM_F_STDNT_TERM.REG_CARD_DT
                         REG_CARD_DT,
                     UM_F_STDNT_TERM.REG_FLG
                         REG_FLG,
                     UM_F_STDNT_TERM.RESET_CUM_STATS_FLG
                         RESET_CUM_STATS_FLG,
                     UM_F_STDNT_TERM.SEL_GROUP
                         SEL_GROUP,
                     UM_F_STDNT_TERM.SSR_ACTV_DT
                         SSR_ACTV_DT,
                     UM_F_STDNT_TERM.STACK_BEGIN_FLG
                         STACK_BEGIN_FLG,                          -- Mar 2018
                     UM_F_STDNT_TERM.STACK_CONTINUE_FLG
                         STACK_CONTINUE_FLG,                       -- Mar 2018
                     UM_F_STDNT_TERM.STACK_READMIT_FLG
                         STACK_READMIT_FLG,                        -- Mar 2018
                     UM_F_STDNT_TERM.STATS_ON_TRN_DT
                         STATS_ON_TRN_DT,
                     UM_F_STDNT_TERM.STDNT_CAR_NUM
                         STDNT_CAR_NUM,
                     UM_F_STDNT_TERM.STUDY_AGREEMENT
                         STUDY_AGREEMENT,
                     UM_F_STDNT_TERM.TERM_ACTV_FLG
                         TERM_ACTV_FLG,
                     UM_F_STDNT_TERM.TERM_ACTV_MAX_TERM_CD
                         TERM_ACTV_MAX_TERM_CD,
                     UM_F_STDNT_TERM.TERM_ACTV_MAX_TERM_SID
                         TERM_ACTV_MAX_TERM_SID,
                     UM_F_STDNT_TERM.TERM_BEGIN_DT
                         TERM_BEGIN_DT,                            -- Mar 2018
                     UM_F_STDNT_TERM.TERM_END_DT
                         TERM_END_DT,                              -- Mar 2018
                     UM_F_STDNT_TERM.TERM_TYPE
                         TERM_TYPE,
                     UM_F_STDNT_TERM.TUIT_CALC_REQ_FLG
                         TUIT_CALC_REQ_FLG,
                     UM_F_STDNT_TERM.TUIT_CALC_DTTM
                         TUIT_CALC_DTTM,
                     UM_F_STDNT_TERM.UGRD_SECOND_DEGR_FLG
                         UGRD_SECOND_DEGR_FLG,
                     UM_F_STDNT_TERM.UMBOS_HON_FLG
                         UMBOS_HON_FLG,
                     UM_F_STDNT_TERM.UNTPRG_CHG_NSLC_DT
                         UNTPRG_CHG_NSLC_DT,
                     UM_F_STDNT_TERM.UNIT_MULTIPLIER
                         UNIT_MULTIPLIER,
                     UM_F_STDNT_TERM.WDN_DT
                         WDN_DT,
                     UM_F_STDNT_TERM.WITHDRAW_CODE
                         WITHDRAW_CODE,
                     UM_F_STDNT_TERM.WITHDRAW_CODE_SD
                         WITHDRAW_CODE_SD,
                     UM_F_STDNT_TERM.WITHDRAW_CODE_LD
                         WITHDRAW_CODE_LD,
                     UM_F_STDNT_TERM.WITHDRAW_REASON
                         WITHDRAW_REASON,
                     UM_F_STDNT_TERM.WITHDRAW_REASON_SD
                         WITHDRAW_REASON_SD,
                     UM_F_STDNT_TERM.WITHDRAW_REASON_LD
                         WITHDRAW_REASON_LD,
                     UM_F_STDNT_TERM.UNDUP_STDNT_CNT
                         UNDUP_STDNT_CNT,
                     UM_F_STDNT_TERM.TERM_ENRL_ORDER
                         TERM_ENRL_ORDER,                         -- July 2016
                     UM_F_STDNT_TERM.UNIT_TAKEN_GPA
                         UNIT_TAKEN_GPA,
                     UM_F_STDNT_TERM.UNIT_TAKEN_NOGPA
                         UNIT_TAKEN_NOGPA,
                     UM_F_STDNT_TERM.GRADE_PTS
                         GRADE_PTS,
                     UM_F_STDNT_TERM.CUR_GPA
                         CUR_GPA,
                     UM_F_STDNT_TERM.UNIT_PASSED_GPA
                         UNIT_PASSED_GPA,
                     UM_F_STDNT_TERM.UNIT_PASSED_NOGPA
                         UNIT_PASSED_NOGPA,
                     UM_F_STDNT_TERM.UNIT_INPROG_GPA
                         UNIT_INPROG_GPA,
                     UM_F_STDNT_TERM.UNIT_INPROG_NOGPA
                         UNIT_INPROG_NOGPA,
                     UM_F_STDNT_TERM.UNIT_TAKEN_PROGRESS
                         UNIT_TAKEN_PROGRESS,
                     UM_F_STDNT_TERM.UNIT_PASSED_PROGRESS
                         UNIT_PASSED_PROGRESS,
                     UM_F_STDNT_TERM.UNIT_AUDIT
                         UNIT_AUDIT,
                     UM_F_STDNT_TERM.TRF_UNIT_TAKEN_GPA
                         TRF_UNIT_TAKEN_GPA,
                     UM_F_STDNT_TERM.TRF_UNIT_TAKEN_NOGPA
                         TRF_UNIT_TAKEN_NOGPA,
                     UM_F_STDNT_TERM.TRF_GRADE_PTS
                         TRF_GRADE_PTS,
                     UM_F_STDNT_TERM.TRF_CUR_GPA
                         TRF_CUR_GPA,
                     UM_F_STDNT_TERM.TRF_UNIT_PASSED_GPA
                         TRF_UNIT_PASSED_GPA,
                     UM_F_STDNT_TERM.TRF_UNIT_PASSED_NOGPA
                         TRF_UNIT_PASSED_NOGPA,
                     UM_F_STDNT_TERM.TRF_UNIT_TOT_GRADED
                         TRF_UNIT_TOT_GRADED,
                     UM_F_STDNT_TERM.TRF_UNIT_TOT
                         TRF_UNIT_TOT,
                     UM_F_STDNT_TERM.TRF_UNIT_ADJUST
                         TRF_UNIT_ADJUST,
                     UM_F_STDNT_TERM.TRF_UNIT_TOT_ADJUSTED
                         TRF_UNIT_TOT_ADJUSTED,
                     UM_F_STDNT_TERM.TRF_UNIT_TEST_CREDIT
                         TRF_UNIT_TEST_CREDIT,
                     UM_F_STDNT_TERM.TRF_UNIT_TRANSFER
                         TRF_UNIT_TRANSFER,
                     UM_F_STDNT_TERM.TRF_UNIT_OTHER
                         TRF_UNIT_OTHER,
                     UM_F_STDNT_TERM.COMB_UNIT_TAKEN_GPA
                         COMB_UNIT_TAKEN_GPA,
                     UM_F_STDNT_TERM.COMB_UNIT_TAKEN_NOGPA
                         COMB_UNIT_TAKEN_NOGPA,
                     UM_F_STDNT_TERM.COMB_GRADE_PTS
                         COMB_GRADE_PTS,
                     UM_F_STDNT_TERM.COMB_CUR_GPA
                         COMB_CUR_GPA,
                     UM_F_STDNT_TERM.COMB_UNIT_PASSED_GPA
                         COMB_UNIT_PASSED_GPA,
                     UM_F_STDNT_TERM.COMB_UNIT_PASSED_NOGPA
                         COMB_UNIT_PASSED_NOGPA,
                     UM_F_STDNT_TERM.COMB_UNIT_PASSED
                         COMB_UNIT_PASSED,
                     UM_F_STDNT_TERM.COMB_UNIT_TOT
                         COMB_UNIT_TOT,
                     UM_F_STDNT_TERM.CUM_UNIT_TAKEN_GPA
                         CUM_UNIT_TAKEN_GPA,
                     UM_F_STDNT_TERM.CUM_UNIT_TAKEN_NOGPA
                         CUM_UNIT_TAKEN_NOGPA,
                     UM_F_STDNT_TERM.CUM_GRADE_PTS
                         CUM_GRADE_PTS,
                     UM_F_STDNT_TERM.CUM_CUR_GPA
                         CUM_CUR_GPA,
                     UM_F_STDNT_TERM.CUM_UNIT_PASSED_GPA
                         CUM_UNIT_PASSED_GPA,
                     UM_F_STDNT_TERM.CUM_UNIT_PASSED_NOGPA
                         CUM_UNIT_PASSED_NOGPA,
                     UM_F_STDNT_TERM.CUM_UNIT_INPROG_GPA
                         CUM_UNIT_INPROG_GPA,
                     UM_F_STDNT_TERM.CUM_UNIT_INPROG_NOGPA
                         CUM_UNIT_INPROG_NOGPA,
                     UM_F_STDNT_TERM.CUM_UNIT_TAKEN_PROGRESS
                         CUM_UNIT_TAKEN_PROGRESS,
                     UM_F_STDNT_TERM.CUM_UNIT_PASSED_PROGRESS
                         CUM_UNIT_PASSED_PROGRESS,
                     UM_F_STDNT_TERM.CUM_UNIT_AUDIT
                         CUM_UNIT_AUDIT,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TAKEN_GPA
                         CUM_TRF_UNIT_TAKEN_GPA,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TAKEN_NOGPA
                         CUM_TRF_UNIT_TAKEN_NOGPA,
                     UM_F_STDNT_TERM.CUM_TRF_GRADE_PTS
                         CUM_TRF_GRADE_PTS,
                     UM_F_STDNT_TERM.CUM_TRF_CUR_GPA
                         CUM_TRF_CUR_GPA,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_PASSED_GPA
                         CUM_TRF_UNIT_PASSED_GPA,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_PASSED_NOGPA
                         CUM_TRF_UNIT_PASSED_NOGPA,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TOT_GRADED
                         CUM_TRF_UNIT_TOT_GRADED,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TOT
                         CUM_TRF_UNIT_TOT,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_ADJUST
                         CUM_TRF_UNIT_ADJUST,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TOT_ADJUSTED
                         CUM_TRF_UNIT_TOT_ADJUSTED,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TEST_CREDIT
                         CUM_TRF_UNIT_TEST_CREDIT,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TRANSFER
                         CUM_TRF_UNIT_TRANSFER,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_OTHER
                         CUM_TRF_UNIT_OTHER,
                     UM_F_STDNT_TERM.CUM_COMB_UNIT_TAKEN_GPA
                         CUM_COMB_UNIT_TAKEN_GPA,
                     UM_F_STDNT_TERM.CUM_COMB_UNIT_TAKEN_NOGPA
                         CUM_COMB_UNIT_TAKEN_NOGPA,
                     UM_F_STDNT_TERM.CUM_COMB_GRADE_PTS
                         CUM_COMB_GRADE_PTS,
                     UM_F_STDNT_TERM.CUM_COMB_CUR_GPA
                         CUM_COMB_CUR_GPA,
                     UM_F_STDNT_TERM.CUM_COMB_UNIT_PASSED_GPA
                         CUM_COMB_UNIT_PASSED_GPA,
                     UM_F_STDNT_TERM.CUM_COMB_UNIT_PASSED_NOGPA
                         CUM_COMB_UNIT_PASSED_NOGPA,
                     UM_F_STDNT_TERM.CUM_COMB_UNIT_PASSED
                         CUM_COMB_UNIT_PASSED,
                     UM_F_STDNT_TERM.CUM_COMB_UNIT_TOT
                         CUM_COMB_UNIT_TOT,
                     UM_F_STDNT_TERM.MAX_UNIT_AUDIT
                         MAX_UNIT_AUDIT,                           -- SMT-8215
                     UM_F_STDNT_TERM.MAX_UNIT_NOGPA
                         MAX_UNIT_NOGPA,                           -- SMT-8215
                     UM_F_STDNT_TERM.MAX_UNIT_TOT
                         MAX_UNIT_TOT,                             -- SMT-8215
                     UM_F_STDNT_TERM.MAX_UNIT_WAIT
                         MAX_UNIT_WAIT,                            -- SMT-8215
                     MAX (UM_F_STDNT_ENRL.ENRLMT_MAX_TERM_CD)
                         ENRLMT_MAX_TERM_CD,
                     MAX (UM_F_STDNT_ENRL.ENRLMT_MAX_TERM_SID)
                         ENRLMT_MAX_TERM_SID,                      -- Mar 2018
                     MAX (UM_F_STDNT_ENRL.ENRLMT_MIN_TERM_CD)
                         ENRLMT_MIN_TERM_CD,                      -- June 2018
                     MAX (UM_F_STDNT_ENRL.ENRLMT_MIN_TERM_SID)
                         ENRLMT_MIN_TERM_SID,                     -- June 2018
                     MAX (UM_F_STDNT_ENRL.ENRLMT_MIN_PERSON_TERM_CD)
                         ENRLMT_MIN_PERSON_TERM_CD,               -- June 2018
                     MAX (UM_F_STDNT_ENRL.ENRLMT_MIN_PERSON_TERM_SID)
                         ENRLMT_MIN_PERSON_TERM_SID,              -- June 2018
                     MAX (UM_F_STDNT_ENRL.ENRLMT_PREV_TERM_CD)
                         ENRLMT_PREV_TERM_CD,                     -- June 2018
                     MAX (UM_F_STDNT_ENRL.ENRLMT_PREV_TERM_SID)
                         ENRLMT_PREV_TERM_SID,                    -- June 2018
                     MAX (UM_F_STDNT_ENRL.ENRL_ADD_DT)
                         ENRL_ADD_MAX_DT,
                     MAX (UM_F_STDNT_ENRL.ENRL_DROP_DT)
                         ENRL_DROP_MAX_DT,
                     SUM (UM_F_STDNT_ENRL.AUDIT_CNT)
                         AUDIT_CNT,
                     (CASE
                          WHEN SUM (UM_F_STDNT_ENRL.AUDIT_CNT) IS NULL
                          THEN
                              'N'
                          WHEN     SUM (UM_F_STDNT_ENRL.AUDIT_CNT) > 0
                               AND SUM (UM_F_STDNT_ENRL.AUDIT_CNT) =
                                   SUM (UM_F_STDNT_ENRL.ENROLL_CNT)
                          THEN
                              'Y'
                          ELSE
                              'N'
                      END)
                         AUDIT_ONLY_FLG,                     -- Added May 2015
                     NVL (SUM (UM_F_STDNT_ENRL.BILLING_UNIT), 0)
                         BILLING_UNIT,
                     NVL (SUM (UM_F_STDNT_ENRL.CE_CREDITS), 0)
                         CE_CREDITS,
                     NVL (SUM (UM_F_STDNT_ENRL.CE_FTE), 0)
                         CE_FTE,
                     (CASE
                          WHEN SUM (UM_F_STDNT_ENRL.CE_CREDITS) IS NULL
                          THEN
                              'N'
                          WHEN     SUM (UM_F_STDNT_ENRL.CE_CREDITS) > 0
                               AND SUM (UM_F_STDNT_ENRL.DAY_CREDITS) = 0
                          THEN
                              'Y'
                          ELSE
                              'N'
                      END)
                         CE_ONLY_FLG,                        -- Added May 2015
                     NVL (SUM (UM_F_STDNT_ENRL.DAY_CREDITS), 0)
                         DAY_CREDITS,
                     NVL (SUM (UM_F_STDNT_ENRL.DAY_FTE), 0)
                         DAY_FTE,
                     (CASE
                          WHEN SUM (UM_F_STDNT_ENRL.DAY_CREDITS) IS NULL
                          THEN
                              'N'
                          WHEN     SUM (UM_F_STDNT_ENRL.DAY_CREDITS) > 0
                               AND SUM (UM_F_STDNT_ENRL.CE_CREDITS) = 0
                          THEN
                              'Y'
                          ELSE
                              'N'
                      END)
                         DAY_ONLY_FLG,                       -- Added May 2015
                     NVL (SUM (UM_F_STDNT_ENRL.CRSE_CNT), 0)
                         CRSE_CNT,
                     NVL (SUM (UM_F_STDNT_ENRL.DROP_CNT), 0)
                         DROP_CNT,
                     NVL (SUM (UM_F_STDNT_ENRL.ENROLL_CNT), 0)
                         ENROLL_CNT,
                     GREATEST (MAX (UM_F_STDNT_ENRL.ENRL_ADD_DT),
                               MAX (UM_F_STDNT_ENRL.ENRL_DROP_DT))
                         ENROLL_DT,                          -- Moved May 2015
                     (CASE
                          WHEN SUM (UM_F_STDNT_ENRL.ENROLL_CNT) IS NULL
                          THEN
                              'N'
                          WHEN SUM (UM_F_STDNT_ENRL.ENROLL_CNT) > 0
                          THEN
                              'Y'
                          ELSE
                              'N'
                      END)
                         ENROLL_FLG,                         -- Added May 2015
                     NVL (SUM (UM_F_STDNT_ENRL.ERN_UNIT), 0)
                         ERN_UNIT,
                     NVL (SUM (UM_F_STDNT_ENRL.IFTE_CNT), 0)
                         IFTE_CNT,
                     NVL (SUM (UM_F_STDNT_ENRL.ONLINE_CNT), 0)
                         ONLINE_CNT,
                     NVL (SUM (UM_F_STDNT_ENRL.ONLINE_CREDITS), 0)
                         ONLINE_CREDITS,
                     NVL (SUM (UM_F_STDNT_ENRL.CE_ONLINE_CREDITS), 0) -- Added Oct 2020
                         CE_ONLINE_CREDITS,
                       NVL (SUM (UM_F_STDNT_ENRL.CE_CREDITS), 0)
                     - NVL (SUM (UM_F_STDNT_ENRL.CE_ONLINE_CREDITS), 0)
                         CE_OTHER_CREDITS,                 -- Added April 2021
                     (CASE
                          WHEN SUM (UM_F_STDNT_ENRL.ONLINE_CNT) IS NULL
                          THEN
                              'N'
                          WHEN     SUM (UM_F_STDNT_ENRL.ONLINE_CNT) > 0
                               AND SUM (UM_F_STDNT_ENRL.ONLINE_CNT) =
                                   SUM (UM_F_STDNT_ENRL.ENROLL_CNT)
                          THEN
                              'Y'
                          ELSE
                              'N'
                      END)
                         ONLINE_ONLY_FLG,                    -- Added May 2015
                     NVL (SUM (UM_F_STDNT_ENRL.PRGRS_UNIT), 0)
                         PRGRS_UNIT,
                     NVL (SUM (UM_F_STDNT_ENRL.PRGRS_FA_UNIT), 0)
                         PRGRS_FA_UNIT,
                     NVL (SUM (UM_F_STDNT_ENRL.TAKEN_UNIT), 0)
                         TAKEN_UNIT,
                     NVL (
                         (  SUM (UM_F_STDNT_ENRL.CE_CREDITS)
                          + SUM (UM_F_STDNT_ENRL.DAY_CREDITS)),
                         0)
                         TOT_CREDITS,
                       NVL (SUM (UM_F_STDNT_ENRL.ONLINE_CREDITS), 0)
                     - NVL (SUM (UM_F_STDNT_ENRL.CE_ONLINE_CREDITS), 0)
                         DAY_ONLINE_CREDITS,                    ----April 2021
                       NVL (SUM (UM_F_STDNT_ENRL.DAY_CREDITS), 0)
                     - (  NVL (SUM (UM_F_STDNT_ENRL.ONLINE_CREDITS), 0)
                        - NVL (SUM (UM_F_STDNT_ENRL.CE_ONLINE_CREDITS), 0))
                         DAY_OTHER_CREDITS,                     ----April 2021
                     NVL (
                         (  SUM (UM_F_STDNT_ENRL.CE_FTE)
                          + SUM (UM_F_STDNT_ENRL.DAY_FTE)),
                         0)
                         TOT_FTE,
                     NVL (SUM (UM_F_STDNT_ENRL.WAIT_CNT), 0)
                         WAIT_CNT,
                     COUNT (*)
                         TERM_COUNT
                FROM UM_F_STDNT_TERM, UM_F_STDNT_ENRL
               WHERE     UM_F_STDNT_ENRL.INSTITUTION_SID =
                         UM_F_STDNT_TERM.INSTITUTION_SID
                     AND UM_F_STDNT_ENRL.ACAD_CAR_SID =
                         UM_F_STDNT_TERM.ACAD_CAR_SID
                     AND UM_F_STDNT_ENRL.TERM_SID = UM_F_STDNT_TERM.TERM_SID
                     AND UM_F_STDNT_ENRL.PERSON_SID =
                         UM_F_STDNT_TERM.PERSON_SID
                     AND UM_F_STDNT_ENRL.SRC_SYS_ID =
                         UM_F_STDNT_TERM.SRC_SYS_ID
            GROUP BY UM_F_STDNT_TERM.TERM_SID,
                     UM_F_STDNT_TERM.PERSON_SID,
                     UM_F_STDNT_TERM.SRC_SYS_ID,
                     UM_F_STDNT_TERM.INSTITUTION_CD,
                     UM_F_STDNT_TERM.ACAD_CAR_CD,
                     UM_F_STDNT_TERM.TERM_CD,
                     UM_F_STDNT_TERM.PERSON_ID,
                     UM_F_STDNT_TERM.INSTITUTION_SID,
                     UM_F_STDNT_TERM.ACAD_CAR_SID,
                     UM_F_STDNT_TERM.STDNT_TERM_KEY,
                     UM_F_STDNT_TERM.PS_STDNT_CAR_NUM,
                     UM_F_STDNT_TERM.PS_PROG_SID,
                     UM_F_STDNT_TERM.PS_EFFDT,
                     UM_F_STDNT_TERM.PS_PROG_STAT_SID,
                     UM_F_STDNT_TERM.PS_PROG_ACN_SID,
                     UM_F_STDNT_TERM.PS_PROG_ACN_RSN_SID,
                     UM_F_STDNT_TERM.PS_ADMIT_TERM_SID,
                     UM_F_STDNT_TERM.PS_EXP_GRAD_TERM_SID,
                     UM_F_STDNT_TERM.PS_DEGR_CHKOUT_LAST_EGT,
                     UM_F_STDNT_TERM.PS_DEGR_CHKOUT_LAST_EGT_LD,
                     UM_F_STDNT_TERM.PS_DEGR_CHKOUT_LAST_EGT_EFFDT,
                     UM_F_STDNT_TERM.MAJ1_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MAJ1_PLAN_SEQUENCE,
                     UM_F_STDNT_TERM.MAJ1_SPLAN1_SID,
                     UM_F_STDNT_TERM.MAJ1_SPLAN2_SID,
                     UM_F_STDNT_TERM.MAJ1_SPLAN3_SID,
                     UM_F_STDNT_TERM.MAJ1_SPLAN4_SID,
                     UM_F_STDNT_TERM.MAJ2_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MAJ2_SPLAN1_SID,
                     UM_F_STDNT_TERM.MAJ3_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MAJ3_SPLAN1_SID,
                     UM_F_STDNT_TERM.MAJ4_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MIN1_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MIN2_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MIN3_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.MIN4_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.OTH1_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.OTH2_ACAD_PLAN_SID,
                     UM_F_STDNT_TERM.ACAD_GRP_ADVIS_SID,
                     UM_F_STDNT_TERM.ACAD_LOAD_APPR_SID,
                     UM_F_STDNT_TERM.ACAD_LOAD_SID,
                     UM_F_STDNT_TERM.STRT_ACAD_LVL_SID,
                     UM_F_STDNT_TERM.END_ACAD_LVL_SID,
                     UM_F_STDNT_TERM.PRJTD_ACAD_LVL_SID,
                     UM_F_STDNT_TERM.PRI_ACAD_PROG_SID,
                     UM_F_STDNT_TERM.ACAD_STNDNG_SID,
                     UM_F_STDNT_TERM.ACAD_STNDNG_TERM_CD_DESC,     -- May 2018
                     UM_F_STDNT_TERM.TERM_ACAD_STNDNG_SID,         -- May 2018
                     UM_F_STDNT_TERM.BILL_CAR_SID,
                     UM_F_STDNT_TERM.FA_LOAD_SID,
                     UM_F_STDNT_TERM.PERSON_ATHL_SID,
                     UM_F_STDNT_TERM.PERSON_SRVC_IND_SID,
                     UM_F_STDNT_TERM.ACAD_CAR_FIRST_FLG,
                     UM_F_STDNT_TERM.ACAD_LOAD_DT,
                     UM_F_STDNT_TERM.ACAD_YR_SID,
                     UM_F_STDNT_TERM.ADMIT_TERM_CD,
                     UM_F_STDNT_TERM.ADMIT_TERM_SD,
                     UM_F_STDNT_TERM.ADMIT_TERM_CD_DESC,
                     UM_F_STDNT_TERM.ADMIT_TYPE_LD,
                     UM_F_STDNT_TERM.CE_ADMIT_FLG,
                     UM_F_STDNT_TERM.CLASS_RANK_NUM,
                     UM_F_STDNT_TERM.CLASS_RANK_TOT,
                     UM_F_STDNT_TERM.COUNTRY,
                     UM_F_STDNT_TERM.ELIG_TO_ENROLL_FLG,
                     UM_F_STDNT_TERM.ENRL_ON_TRN_DT,
                     UM_F_STDNT_TERM.EXT_ORG_ID,
                     UM_F_STDNT_TERM.FA_APPL_FLG,
                     UM_F_STDNT_TERM.FA_ELIG_FLG,
                     UM_F_STDNT_TERM.FA_PELL_ELIGIBILITY,
                     UM_F_STDNT_TERM.FA_PELL_DISB_FLAG,
                     UM_F_STDNT_TERM.FA_LOAN_DISB_FLAG,
                     UM_F_STDNT_TERM.FA_STATS_CALC_REQ_FLG,
                     UM_F_STDNT_TERM.FA_STATS_CALC_DTTM,
                     UM_F_STDNT_TERM.FORM_OF_STUDY,
                     UM_F_STDNT_TERM.FORM_OF_STUDY_SD,
                     UM_F_STDNT_TERM.FORM_OF_STUDY_LD,
                     UM_F_STDNT_TERM.FULLY_ENRL_DT,
                     UM_F_STDNT_TERM.FULLY_GRADED_DT,
                     UM_F_STDNT_TERM.LAST_ATTND_DT,
                     UM_F_STDNT_TERM.LOCK_IN_AMT,
                     UM_F_STDNT_TERM.LOCK_IN_DT,
                     UM_F_STDNT_TERM.MAX_CRSE_CNT,
                     UM_F_STDNT_TERM.NSLDS_LOAN_YEAR,
                     UM_F_STDNT_TERM.NSLDS_LOAN_YEAR_SD,
                     UM_F_STDNT_TERM.NSLDS_LOAN_YEAR_LD,
                     UM_F_STDNT_TERM.OVRD_ACAD_LVL_PROJ_FLG,
                     UM_F_STDNT_TERM.OVRD_ACAD_LVL_ALL_FLG,
                     UM_F_STDNT_TERM.OVRD_BILL_UNITS_FLG,
                     UM_F_STDNT_TERM.OVRD_INIT_ADD_FEE_FLG,
                     UM_F_STDNT_TERM.OVRD_INIT_ENR_FEE_FLG,
                     UM_F_STDNT_TERM.OVRD_MAX_UNITS_FLG,
                     UM_F_STDNT_TERM.OVRD_TUIT_GROUP,
                     UM_F_STDNT_TERM.OVRD_WDRW_SCHED,
                     UM_F_STDNT_TERM.PRJTD_BILL_UNIT,
                     UM_F_STDNT_TERM.PRO_RATA_ELIG_FLG,
                     UM_F_STDNT_TERM.REFUND_PCT,
                     UM_F_STDNT_TERM.REFUND_SCHEME,
                     UM_F_STDNT_TERM.REG_CARD_DT,
                     UM_F_STDNT_TERM.REG_FLG,
                     UM_F_STDNT_TERM.RESET_CUM_STATS_FLG,
                     UM_F_STDNT_TERM.SEL_GROUP,
                     UM_F_STDNT_TERM.SSR_ACTV_DT,
                     UM_F_STDNT_TERM.STACK_BEGIN_FLG,              -- Mar 2018
                     UM_F_STDNT_TERM.STACK_CONTINUE_FLG,           -- Mar 2018
                     UM_F_STDNT_TERM.STACK_READMIT_FLG,            -- Mar 2018
                     UM_F_STDNT_TERM.STATS_ON_TRN_DT,
                     UM_F_STDNT_TERM.STDNT_CAR_NUM,
                     UM_F_STDNT_TERM.STUDY_AGREEMENT,
                     UM_F_STDNT_TERM.TERM_ACTV_FLG,
                     UM_F_STDNT_TERM.TERM_ACTV_MAX_TERM_CD,
                     UM_F_STDNT_TERM.TERM_ACTV_MAX_TERM_SID,
                     UM_F_STDNT_TERM.TERM_BEGIN_DT,                -- Mar 2018
                     UM_F_STDNT_TERM.TERM_END_DT,                  -- Mar 2018
                     UM_F_STDNT_TERM.TERM_TYPE,
                     UM_F_STDNT_TERM.TUIT_CALC_REQ_FLG,
                     UM_F_STDNT_TERM.TUIT_CALC_DTTM,
                     UM_F_STDNT_TERM.UGRD_SECOND_DEGR_FLG,
                     UM_F_STDNT_TERM.UMBOS_HON_FLG,
                     UM_F_STDNT_TERM.UNTPRG_CHG_NSLC_DT,
                     UM_F_STDNT_TERM.UNIT_MULTIPLIER,
                     UM_F_STDNT_TERM.WDN_DT,
                     UM_F_STDNT_TERM.WITHDRAW_CODE,
                     UM_F_STDNT_TERM.WITHDRAW_CODE_SD,
                     UM_F_STDNT_TERM.WITHDRAW_CODE_LD,
                     UM_F_STDNT_TERM.WITHDRAW_REASON,
                     UM_F_STDNT_TERM.WITHDRAW_REASON_SD,
                     UM_F_STDNT_TERM.WITHDRAW_REASON_LD,
                     UM_F_STDNT_TERM.UNDUP_STDNT_CNT,
                     UM_F_STDNT_TERM.TERM_ENRL_ORDER,             -- July 2016
                     UM_F_STDNT_TERM.UNIT_TAKEN_GPA,
                     UM_F_STDNT_TERM.UNIT_TAKEN_NOGPA,
                     UM_F_STDNT_TERM.GRADE_PTS,
                     UM_F_STDNT_TERM.CUR_GPA,
                     UM_F_STDNT_TERM.UNIT_PASSED_GPA,
                     UM_F_STDNT_TERM.UNIT_PASSED_NOGPA,
                     UM_F_STDNT_TERM.UNIT_INPROG_GPA,
                     UM_F_STDNT_TERM.UNIT_INPROG_NOGPA,
                     UM_F_STDNT_TERM.UNIT_TAKEN_PROGRESS,
                     UM_F_STDNT_TERM.UNIT_PASSED_PROGRESS,
                     UM_F_STDNT_TERM.UNIT_AUDIT,
                     UM_F_STDNT_TERM.TRF_UNIT_TAKEN_GPA,
                     UM_F_STDNT_TERM.TRF_UNIT_TAKEN_NOGPA,
                     UM_F_STDNT_TERM.TRF_GRADE_PTS,
                     UM_F_STDNT_TERM.TRF_CUR_GPA,
                     UM_F_STDNT_TERM.TRF_UNIT_PASSED_GPA,
                     UM_F_STDNT_TERM.TRF_UNIT_PASSED_NOGPA,
                     UM_F_STDNT_TERM.TRF_UNIT_TOT_GRADED,
                     UM_F_STDNT_TERM.TRF_UNIT_TOT,
                     UM_F_STDNT_TERM.TRF_UNIT_ADJUST,
                     UM_F_STDNT_TERM.TRF_UNIT_TOT_ADJUSTED,
                     UM_F_STDNT_TERM.TRF_UNIT_TEST_CREDIT,
                     UM_F_STDNT_TERM.TRF_UNIT_TRANSFER,
                     UM_F_STDNT_TERM.TRF_UNIT_OTHER,
                     UM_F_STDNT_TERM.COMB_UNIT_TAKEN_GPA,
                     UM_F_STDNT_TERM.COMB_UNIT_TAKEN_NOGPA,
                     UM_F_STDNT_TERM.COMB_GRADE_PTS,
                     UM_F_STDNT_TERM.COMB_CUR_GPA,
                     UM_F_STDNT_TERM.COMB_UNIT_PASSED_GPA,
                     UM_F_STDNT_TERM.COMB_UNIT_PASSED_NOGPA,
                     UM_F_STDNT_TERM.COMB_UNIT_PASSED,
                     UM_F_STDNT_TERM.COMB_UNIT_TOT,
                     UM_F_STDNT_TERM.CUM_UNIT_TAKEN_GPA,
                     UM_F_STDNT_TERM.CUM_UNIT_TAKEN_NOGPA,
                     UM_F_STDNT_TERM.CUM_GRADE_PTS,
                     UM_F_STDNT_TERM.CUM_CUR_GPA,
                     UM_F_STDNT_TERM.CUM_UNIT_PASSED_GPA,
                     UM_F_STDNT_TERM.CUM_UNIT_PASSED_NOGPA,
                     UM_F_STDNT_TERM.CUM_UNIT_INPROG_GPA,
                     UM_F_STDNT_TERM.CUM_UNIT_INPROG_NOGPA,
                     UM_F_STDNT_TERM.CUM_UNIT_TAKEN_PROGRESS,
                     UM_F_STDNT_TERM.CUM_UNIT_PASSED_PROGRESS,
                     UM_F_STDNT_TERM.CUM_UNIT_AUDIT,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TAKEN_GPA,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TAKEN_NOGPA,
                     UM_F_STDNT_TERM.CUM_TRF_GRADE_PTS,
                     UM_F_STDNT_TERM.CUM_TRF_CUR_GPA,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_PASSED_GPA,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_PASSED_NOGPA,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TOT_GRADED,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TOT,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_ADJUST,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TOT_ADJUSTED,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TEST_CREDIT,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_TRANSFER,
                     UM_F_STDNT_TERM.CUM_TRF_UNIT_OTHER,
                     UM_F_STDNT_TERM.CUM_COMB_UNIT_TAKEN_GPA,
                     UM_F_STDNT_TERM.CUM_COMB_UNIT_TAKEN_NOGPA,
                     UM_F_STDNT_TERM.CUM_COMB_GRADE_PTS,
                     UM_F_STDNT_TERM.CUM_COMB_CUR_GPA,
                     UM_F_STDNT_TERM.CUM_COMB_UNIT_PASSED_GPA,
                     UM_F_STDNT_TERM.CUM_COMB_UNIT_PASSED_NOGPA,
                     UM_F_STDNT_TERM.CUM_COMB_UNIT_PASSED,
                     UM_F_STDNT_TERM.CUM_COMB_UNIT_TOT,
                     UM_F_STDNT_TERM.MAX_UNIT_AUDIT,
                     UM_F_STDNT_TERM.MAX_UNIT_NOGPA,
                     UM_F_STDNT_TERM.MAX_UNIT_TOT,
                     UM_F_STDNT_TERM.MAX_UNIT_WAIT);
