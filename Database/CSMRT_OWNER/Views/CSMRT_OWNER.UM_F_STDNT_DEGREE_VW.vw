DROP VIEW CSMRT_OWNER.UM_F_STDNT_DEGREE_VW
/

--
-- UM_F_STDNT_DEGREE_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_STDNT_DEGREE_VW
BEQUEATH DEFINER
AS 
SELECT /*+ OPT_ESTIMATE(TABLE UM_F_STDNT_DEGREE MIN=100000) */
          PERSON_SID,
          INSTITUTION_SID,
          ACAD_CAR_SID,
          STDNT_CAR_NUM,
          TERM_SID,
          ACAD_PROG_SID,
          ACAD_PLAN_SID,
          ACAD_SPLAN_SID,
          DEGREE_NBR,
          HONORS_NBR,
          SRC_SYS_ID,
          PERSON_ID,
          INSTITUTION_CD,
          ACAD_CAR_CD,
          TERM_CD,
          ACAD_PROG_CD,
          ACAD_PLAN_CD,
          ACAD_SPLAN_CD,
          DEG_SID,
          DEG_HONORS_SID,
          HONORS_AWD_DT_SID,
          HONORS_AWD_DT,
          COMPL_TERM_SID,
          CONF_DT_SID,
          CONF_DT,
          HONORS_PREFIX_SID,
          HONORS_SUFFIX_SID,
          GPA_DEGREE,
          CLASS_RANK_NBR,
          CLASS_RANK_TOT,
          ACAD_DEGR_STAT_SID,
          DEGR_STAT_DT_SID,
          DEGR_STAT_DT,
          PLAN_SEQUENCE,
          PLAN_DEGR_STATUS,
          PLN_DEG_ST_DT_SID,
          PLN_DEG_ST_DT,
          PLAN_OVERRIDE_FLG,
          PLAN_DIPLOMA_DESCR,
          PLAN_TRNSCR_DESCR,
          PLN_HONRS_PREF_SID,
          PLN_HONRS_SUFF_SID,
          GPA_PLAN,
          PLN_CLASS_RANK_NBR,
          PLN_CLASS_RANK_TOT,
          SPLAN_SEQUENCE,
          SPLAN_OVERRIDE_FLG,
          SPLAN_DIPLOMA_DESC,
          SPLAN_TRNSCR_DESCR,
          SPLN_HNRS_PREF_SID,
          SPLN_HNRS_SUFF_SID,
          NVL ((SELECT min(TERM_BEGIN_DT) TERM_BEGIN_DT
                  FROM PS_D_TERM T
                 WHERE T.TERM_SID = UM_F_STDNT_DEGREE.TERM_SID),trunc(SYSDATE)) TERM_BEGIN_DT,             -- Temp!!!
          NVL ((SELECT min(TERM_END_DT) TERM_END_DT
                  FROM PS_D_TERM T
                 WHERE T.TERM_SID = UM_F_STDNT_DEGREE.TERM_SID),trunc(SYSDATE)) TERM_END_DT,             -- Temp!!!
          AWARD_CNT,
          HONORS_CNT,
          REVOKE_CNT,
          PRIM_PROG_MAJOR1_ORDER,
          DEG_NAME,
          DEG_FIRST_NAME,
          DEG_MIDDLE_NAME,
          DEG_LAST_NAME
     FROM UM_F_STDNT_DEGREE
--    WHERE ROWNUM < 1000000000
/
