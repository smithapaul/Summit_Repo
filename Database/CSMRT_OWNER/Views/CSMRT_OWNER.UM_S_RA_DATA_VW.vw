DROP VIEW CSMRT_OWNER.UM_S_RA_DATA_VW
/

--
-- UM_S_RA_DATA_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_S_RA_DATA_VW
BEQUEATH DEFINER
AS 
SELECT RUN_DT,
           RUN_DT     EFFDT_START,
           RUN_DT     EFFDT_END,
           INSTITUTION_CD,
           TERM_CD,
           ADM_APPL_NBR,
           SRC_SYS_ID,
           ACAD_CAR_CD,
           ADMIT_TYPE_GRP,
           ADMIT_TYPE_ID,
           APPL_CNTR_ID,
           ACAD_PROG_CD,
           ACAD_PLAN_CD,
           ACAD_SPLAN_CD,
           EDU_LVL_CTGRY,
           EDU_LVL_CD,
           PROG_STAT_CD,
           PROG_ACN_CD,
           PROG_ACN_RSN_CD,
           ACTION_DT,
           APPL_CNT,
           APPL_COMPLETE_CNT,
           ADMIT_CNT,
           DENY_CNT,
           WAIT_CNT,
           DEPOSIT_CNT,
           MATRIC_CNT,
           ENROLL_CNT,
           PERSON_ID,
           GENDER_CD,
           AGE,
           COUNTRY,
           CITIZENSHIP_STATUS,
           ETHNIC_GRP_FED_CD,
           ETHNIC_GRP_ST_CD,
           RSDNCY_ID,
           TUITION_RSDNCY_ID,
           ACAD_GRP_CD,
           ABTS_FLAG,
           BSMS_FLAG
      FROM UM_S_RA_DATA
/
