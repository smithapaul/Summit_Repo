DROP VIEW CSMRT_OWNER.UM_S_SR_DATA_VW
/

--
-- UM_S_SR_DATA_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_S_SR_DATA_VW
BEQUEATH DEFINER
AS 
SELECT RUN_DT,
           RUN_DT     EFFDT_START,
           RUN_DT     EFFDT_END,
           INSTITUTION_CD,
           ACAD_CAR_CD,
           TERM_CD,
           PERSON_ID,
           STDNT_CAR_NUM,
           ACAD_PROG_CD,
           ACAD_PLAN_CD,
           ACAD_SPLAN_CD,
           SRC_SYS_ID,
           ONLINE_CREDITS,
           CE_ONLINE_CREDITS,       -- Nov 2020
           TOT_CREDITS,
           DAY_CREDITS,
           CE_CREDITS,
           DAY_ONLY_FLG,
           ONLINE_ONLY_FLG,
           ENROLL_FLG,
           CE_ONLY_FLG,
           AUDIT_ONLY_FLG,
           UNDUP_STDNT_CNT,
           UGRD_SECOND_DEGR_FLG,
           STACK_READMIT_FLG,
           STACK_BEGIN_FLG,
           DEGREE_SEEKING_FLG,
           STACK_CONTINUE_FLG,
           CERTIFICATE_ONLY_FLG,
           STDNT_ATTR_VAL_LD,
           STDNT_ATTR_VALUE,
           ACAD_GRP_CD,
           EDU_LVL_CD,
           ACAD_PLAN_TYPE_CD,
           PROG_STAT_CD,
           ETHNIC_GRP_FED_CD,
           GENDER_CD,
           ETHNIC_GRP_ST_CD,
           RSDNCY_ID,
           ACAD_LVL_CD,
           COUNTRY,
           CITIZENSHIP_STATUS,
           PROG_ACN_CD,
           PROG_ACN_RSN_CD
      FROM UM_S_SR_DATA
/
