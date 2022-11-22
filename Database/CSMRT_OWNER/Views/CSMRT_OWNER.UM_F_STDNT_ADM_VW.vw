DROP VIEW CSMRT_OWNER.UM_F_STDNT_ADM_VW
/

--
-- UM_F_STDNT_ADM_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_STDNT_ADM_VW
BEQUEATH DEFINER
AS 
SELECT /*+ OPT_ESTIMATE(TABLE UM_F_STDNT_ADM MIN=100000) */
           PERSON_SID,
           INSTITUTION_SID,
           ACAD_CAR_SID,
           STU_CAR_NBR,
           STU_CAR_NBR_SR,
           ADM_APPL_NBR,
           APPL_PROG_NBR,
           SR_ACAD_PROG_SID,
           SR_ACAD_PLAN_SID,
           SR_ACAD_SPLAN_SID,
           ADM_ACAD_PROG_SID,
           ADM_ACAD_PLAN_SID,
           ADM_ACAD_SPLAN_SID,
           SRC_SYS_ID,
           INSTITUTION_CD,
           ADMIT_TERM_SID,
           ADMIT_TYPE_SID,
           ACAD_LVL_SID,
           ACAD_LOAD_SID,
           PROG_STAT_SID,
           PROG_ACN_SID,
           PROG_ACN_RSN_SID,
           ACTION_DT,
           APPL_DT,
           APPL_CNTR_SID,
           APPL_MTHD_SID,
           EXT_DEG_SID,
           EXT_DEG_DT,
           EXT_DEG_STAT_ID,
           EXT_DEG_STAT_SD,
           EXT_DEG_STAT_LD,
           FIN_AID_INTEREST,
           HOUSING_INTEREST,
           HOUSING_INTEREST_SD,
           HOUSING_INTEREST_LD,
           LST_SCHL_ATTND_SID,
           LST_SCHL_GRDDT_SID,
           NOTIFICATION_PLAN,
           NOTIFICATION_PLAN_SD,
           NOTIFICATION_PLAN_LD,
           NVL ((SELECT MIN (TERM_BEGIN_DT)     TERM_BEGIN_DT
                   FROM PS_D_TERM T
                  WHERE T.TERM_SID = UM_F_STDNT_ADM.ADMIT_TERM_SID),
                TRUNC (SYSDATE))    TERM_BEGIN_DT,                  -- Temp!!!
           NVL ((SELECT MIN (TERM_END_DT)     TERM_END_DT
                   FROM PS_D_TERM T
                  WHERE T.TERM_SID = UM_F_STDNT_ADM.ADMIT_TERM_SID),
                TRUNC (SYSDATE))    TERM_END_DT,                    -- Temp!!!
           UM_BHE,
           UM_BHE_SD,
           UM_BHE_LD,
           UM_BHE_ENG,
           UM_BHE_SOCSCI,
           UM_BHE_SCI,
           UM_BHE_MATH,
           UM_BHE_ELT,
           UM_BHE_FRLG,
           UM_BHE_CMPLT,
           UM_BHE_EXP_VOCTEC,
           UM_BHE_EXP_ESL,
           UM_BHE_EXP_INTL,
           UM_BHE_PRECOLLEGE,
           UM_BHE_EXP_LD,
           UM_BHE_TRANS_CR,
           UM_BHE_TRANS_GPA,
           UM_RA_TA_INTEREST,
           UM_RA_TA_INTEREST_SD,
           UM_RA_TA_INTEREST_LD,
           UM_TCA_COMPLETE,
           UM_TCA_CREDITS,
           EXT_GPA,
           CONVERTED_GPA,
--           UM_CUM_CREDIT,
--           UM_CUM_GPA,
--           UM_CUM_QP,
case when INSTITUTION_CD = 'UMBOS' then UM_CUM_CREDIT else UM_CUM_CREDIT_AGG end UM_CUM_CREDIT,     -- Aug 2022 
case when INSTITUTION_CD = 'UMBOS' then UM_CUM_GPA else UM_CUM_GPA_AGG end UM_CUM_GPA,              -- Aug 2022
case when INSTITUTION_CD = 'UMBOS' then UM_CUM_QP else UM_CUM_QP_AGG end UM_CUM_QP,                 -- Aug 2022 
           UM_CUM_CREDIT_AGG,       -- Aug 2022 
           UM_CUM_GPA_AGG,          -- Aug 2022 
           UM_CUM_QP_AGG,           -- Aug 2022 
           UM_GPA_EXCLUDE_FLG,
           UM_EXT_ORG_CR,
           UM_EXT_ORG_QP,
           UM_EXT_ORG_GPA,
           UM_EXT_ORG_CNV_CR,
           UM_EXT_ORG_CNV_GPA,
           UM_EXT_ORG_CNV_QP,
           UM_GPA_OVRD_FLG,
           UM_1_OVRD_HSGPA_FLG,
           UM_CONVERT_GPA,
           TEST_EFFDT,
           ACT_COMP_SCORE,
           ACT_CONV_SCORE,
           GMAT_TOTAL_SCORE,
           GRE_COMB_DECILE,
           GRE_ANLY_SCORE,
           GRE_QUAN_SCORE,
           GRE_VERB_SCORE,
           IELTS_BAND_SCORE,
           LSAT_COMP_SCORE,
           SAT_COMB_DECILE,
           SAT_MATH_SCORE,
           SAT_VERB_SCORE,
           SAT_CONV_SCORE,
           TOEFL_IBTT_SCORE,
           UMDAR_INDEX_SCORE,
           UMLOW_INDEX_SCORE,
           UM_EXT_OR_MTSC_GPA,
           MS_CONVERT_GPA,
           UM_CA_FIRST_GEN,
           MAX_DATA_ROW,        -- Aug 2022 
           DATA_ORIGIN,
           ABTS_FLAG,
           BSMS_FLAG,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM

      FROM CSMRT_OWNER.UM_F_STDNT_ADM
/
