DROP VIEW CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT_VW
/

--
-- UM_F_ADM_APPL_CLASS_ENRLMT_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT_VW
BEQUEATH DEFINER
AS 
SELECT /*+ OPT_ESTIMATE(TABLE UM_F_ADM_APPL_CLASS_ENRLMT MIN=100000) */
          ADM_APPL_SID,
          SESSION_SID,
          PERSON_SID,
          CLASS_NUM,
          SRC_SYS_ID,
          INSTITUTION_SID,
          INSTITUTION_CD,
          ACAD_CAR_SID,
          TERM_SID,
          CLASS_SID,
          CLASS_MTG_PAT_SID_P1,
          CLASS_MTG_PAT_SID_P2,
          ENRLMT_REAS_SID,
          ENRLMT_STAT_SID,
          GRADE_SID,
          PRI_CLASS_INSTRCTR_SID,
          REPEAT_SID,
          ENRL_ADD_DT,
          ENRL_DROP_DT,
          ENRLMT_STAT_DT,
          GRADE_DT,
          GRADE_BASIS_DT,
          REPEAT_DT,
          REPEAT_FLG,
          CLASS_CD,
          CLASS_SECTION_CD,
          GRADE_PTS,
          BILLING_UNIT,
          TAKEN_UNIT,
          PRGRS_UNIT,
          ERN_UNIT,
          CE_CREDITS,
          CE_FTE,
          DAY_CREDITS,
          DAY_FTE,
          ENROLL_CNT,
          DROP_CNT,
          WAIT_CNT,
          IFTE_CNT
     FROM CSMRT_OWNER.UM_F_ADM_APPL_CLASS_ENRLMT
--    WHERE ROWNUM < 1000000000
/
