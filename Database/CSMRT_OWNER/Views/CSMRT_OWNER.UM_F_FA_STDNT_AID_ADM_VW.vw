DROP VIEW CSMRT_OWNER.UM_F_FA_STDNT_AID_ADM_VW
/

--
-- UM_F_FA_STDNT_AID_ADM_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_FA_STDNT_AID_ADM_VW
BEQUEATH DEFINER
AS 
SELECT /*+ OPT_ESTIMATE(TABLE UM_F_FA_STDNT_AID_ADM MIN=100000) */
           INSTITUTION_CD,
           PERSON_ID,
           INSTITUTION_SID,
           PERSON_SID,
           ACAD_CAR_SID,
           STU_CAR_NBR,
           ADM_APPL_NBR,
           APPL_PROG_NBR,
           ACAD_PROG_SID,
           ACAD_PLAN_SID,
           ACAD_SPLAN_SID,
           EFFDT,
           EFFSEQ,
           SRC_SYS_ID,
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
           LST_SCHL_GRDDT,
           NOTIFICATION_PLAN,
           NOTIFICATION_PLAN_SD,
           NOTIFICATION_PLAN_LD,
           NVL ((SELECT MIN (TERM_BEGIN_DT)     TERM_BEGIN_DT
                   FROM PS_D_TERM T
                  WHERE T.TERM_SID = UM_F_FA_STDNT_AID_ADM.ADMIT_TERM_SID),
                TRUNC (SYSDATE))    TERM_BEGIN_DT,                  -- Temp!!!
           NVL ((SELECT MIN (TERM_END_DT)     TERM_END_DT
                   FROM PS_D_TERM T
                  WHERE T.TERM_SID = UM_F_FA_STDNT_AID_ADM.ADMIT_TERM_SID),
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
           UM_CA_FIRST_GEN,                                  -- Added Mar 2017
           UM_RA_TA_INTEREST,
           UM_RA_TA_INTEREST_SD,
           UM_RA_TA_INTEREST_LD,
           STU_CAR_NBR_SR,                                   -- Added Mar 2016
           UM_TCA_COMPLETE,
           UM_TCA_CREDITS,
           EXT_SUMM_TYPE_ID,                                       -- Jan 2017
           EXT_GPA,
           CONVERTED_GPA,
--           UM_CUM_CREDIT,
--           UM_CUM_GPA,
--           UM_CUM_QP,
case when INSTITUTION_CD = 'UMBOS' then UM_CUM_CREDIT else UM_CUM_CREDIT_AGG end UM_CUM_CREDIT,     -- Aug 2022 
case when INSTITUTION_CD = 'UMBOS' then UM_CUM_GPA else UM_CUM_GPA_AGG end UM_CUM_GPA,              -- Aug 2022
case when INSTITUTION_CD = 'UMBOS' then UM_CUM_QP else UM_CUM_QP_AGG end UM_CUM_QP,                 -- Aug 2022 
           UM_CUM_CREDIT_AGG,           -- Aug 2022 
           UM_CUM_GPA_AGG,              -- Aug 2022 
           UM_CUM_QP_AGG,               -- Aug 2022 
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
           SAT_TOTAL_UM_SCORE,                                     -- Jan 2017
           SAT_TOTAL_1600_CONV_SCORE,                              -- Jan 2017
           SAT_CONV_2016_SCORE,                                    -- Jan 2017
           UM_EXT_OR_MTSC_GPA,                                     -- Sep 2019
           MS_CONVERT_GPA,                                         -- Sep 2019
           MAX_DATA_ROW,                                           -- Aug 2022 
           DATA_ORIGIN,                                            -- Sep 2019
           ABTS_FLAG,
           BSMS_FLAG, 
           CREATED_EW_DTTM,                                        -- Sep 2019
           LASTUPD_EW_DTTM                                         -- Sep 2019
      FROM CSMRT_OWNER.UM_F_FA_STDNT_AID_ADM
/
