DROP VIEW CSMRT_OWNER.UM_F_STDNT_ENRL_HIST_VW
/

--
-- UM_F_STDNT_ENRL_HIST_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_STDNT_ENRL_HIST_VW
BEQUEATH DEFINER
AS 
WITH
        R1
        AS
            (SELECT DISTINCT INSTITUTION_CD,
                             ACAD_CAREER,
                             TERM_CD,
                             PERSON_ID,
                             CLASS_NBR
               FROM UM_F_STDNT_ENRL_REQ
              WHERE     ENRL_REQ_ACTION IN ('D')
                    AND ENRL_REQ_DETL_STAT IN ('S', 'M')
             MINUS
             SELECT DISTINCT INSTITUTION_CD,
                             ACAD_CAREER,
                             TERM_CD,
                             PERSON_ID,
                             CLASS_NBR
               FROM UM_F_STDNT_ENRL_REQ
              WHERE     ENRL_REQ_ACTION IN ('E')
                    AND ENRL_REQ_DETL_STAT IN ('S', 'M')),
        E1
        AS
            (SELECT INSTITUTION_CD,
                    ACAD_CAR_CD,
                    TERM_CD,
                    PERSON_ID,
                    CLASS_NUM,
                    ENRL_ADD_DT     ENRL_DT,
                    SRC_SYS_ID,
                    'N'             REQ_FLG,
                    TERM_SID,
                    PERSON_SID,
                    1               CLASS_ENRL_CNT,
                    TAKEN_UNIT
               FROM UM_F_CLASS_ENRLMT F
             -- where ((ENRL_ADD_DT <> ENRL_DROP_DT) or ENRL_DROP_DT is NULL)
             UNION ALL
             SELECT INSTITUTION_CD,
                    ACAD_CAR_CD,
                    TERM_CD,
                    PERSON_ID,
                    CLASS_NUM,
                    ENRL_DROP_DT          ENRL_DT,
                    F.SRC_SYS_ID,
                    'N'                   REQ_FLG,
                    TERM_SID,
                    PERSON_SID,
                    -1                    CLASS_ENRL_CNT,
                    (-1 * TAKEN_UNIT)     TAKEN_UNIT
               FROM UM_F_CLASS_ENRLMT F, PS_D_ENRLMT_STAT S
              WHERE     F.ENRLMT_STAT_SID = S.ENRLMT_STAT_SID
                    AND S.ENRLMT_STAT_ID = 'D'
             --   and ENRL_ADD_DT <> ENRL_DROP_DT
             UNION ALL
             SELECT INSTITUTION_CD,
                    ACAD_CAREER,
                    TERM_CD,
                    PERSON_ID,
                    CLASS_NBR,
                    TRUNC (DTTM_STAMP_SEC)
                        ENRL_DT,
                    SRC_SYS_ID,
                    'Y'
                        REQ_FLG,
                    TERM_SID,
                    PERSON_SID,
                    (DECODE (ENRL_REQ_ACTION, 'D', -1, 1))
                        CLASS_ENRL_CNT,
                    (DECODE (ENRL_REQ_ACTION, 'D', -1 * UNT_TAKEN, UNT_TAKEN))
                        TAKEN_UNIT
               FROM UM_F_STDNT_ENRL_REQ R
              WHERE     ENRL_REQ_ACTION IN ('E', 'D')
                    AND ENRL_REQ_DETL_STAT IN ('S', 'M')
                    AND NOT EXISTS
                            (SELECT 1
                               FROM R1
                              WHERE     R.INSTITUTION_CD = R1.INSTITUTION_CD
                                    AND R.ACAD_CAREER = R1.ACAD_CAREER
                                    AND R.TERM_CD = R1.TERM_CD
                                    AND R.PERSON_ID = R1.PERSON_ID
                                    AND R.CLASS_NBR = R1.CLASS_NBR)),
        E2
        AS
            (SELECT INSTITUTION_CD,
                    ACAD_CAR_CD,
                    TERM_CD,
                    PERSON_ID,
                    CLASS_NUM,
                    ENRL_DT,
                    SRC_SYS_ID,
                    REQ_FLG,
                    DENSE_RANK ()
                        OVER (PARTITION BY INSTITUTION_CD,
                                           ACAD_CAR_CD,
                                           TERM_CD,
                                           PERSON_ID,
                                           CLASS_NUM,
                                           SRC_SYS_ID
                              ORDER BY REQ_FLG DESC)
                        SRC_RANK,
                    TERM_SID,
                    PERSON_SID,
                    CLASS_ENRL_CNT,
                    TAKEN_UNIT
               FROM E1),
        E3
        AS
            (  SELECT INSTITUTION_CD,
                      ACAD_CAR_CD,
                      TERM_CD,
                      PERSON_ID,
                      ENRL_DT,
                      SRC_SYS_ID,
                      TERM_SID,
                      PERSON_SID,
                      SUM (CLASS_ENRL_CNT)     CLASS_ENRL_CNT,
                      SUM (TAKEN_UNIT)         TAKEN_UNIT
                 FROM E2
                WHERE SRC_RANK = 1
             GROUP BY INSTITUTION_CD,
                      ACAD_CAR_CD,
                      TERM_CD,
                      PERSON_ID,
                      ENRL_DT,
                      SRC_SYS_ID,
                      TERM_SID,
                      PERSON_SID),
        E4
        AS
            (SELECT INSTITUTION_CD,
                    ACAD_CAR_CD,
                    TERM_CD,
                    PERSON_ID,
                    ENRL_DT,
                    SRC_SYS_ID,
                    TERM_SID,
                    PERSON_SID,
                    NVL (
                        SUM (CLASS_ENRL_CNT)
                            OVER (
                                PARTITION BY INSTITUTION_CD,
                                             ACAD_CAR_CD,
                                             TERM_CD,
                                             PERSON_ID,
                                             SRC_SYS_ID
                                ORDER BY ENRL_DT
                                ROWS BETWEEN UNBOUNDED PRECEDING
                                     AND     1 PRECEDING),
                        0)
                        CLASS_ENRL_CNT_PREV_SUM,
                    CLASS_ENRL_CNT,
                    NVL (
                        SUM (CLASS_ENRL_CNT)
                            OVER (
                                PARTITION BY INSTITUTION_CD,
                                             ACAD_CAR_CD,
                                             TERM_CD,
                                             PERSON_ID,
                                             SRC_SYS_ID
                                ORDER BY ENRL_DT
                                ROWS BETWEEN UNBOUNDED PRECEDING
                                     AND     CURRENT ROW),
                        0)
                        CLASS_ENRL_CNT_SUM,
                    NVL (
                        SUM (TAKEN_UNIT)
                            OVER (
                                PARTITION BY INSTITUTION_CD,
                                             ACAD_CAR_CD,
                                             TERM_CD,
                                             PERSON_ID,
                                             SRC_SYS_ID
                                ORDER BY ENRL_DT
                                ROWS BETWEEN UNBOUNDED PRECEDING
                                     AND     1 PRECEDING),
                        0)
                        TAKEN_UNIT_PREV_SUM,
                    TAKEN_UNIT,
                    NVL (
                        SUM (TAKEN_UNIT)
                            OVER (
                                PARTITION BY INSTITUTION_CD,
                                             ACAD_CAR_CD,
                                             TERM_CD,
                                             PERSON_ID,
                                             SRC_SYS_ID
                                ORDER BY ENRL_DT
                                ROWS BETWEEN UNBOUNDED PRECEDING
                                     AND     CURRENT ROW),
                        0)
                        TAKEN_UNIT_SUM
               FROM E3)
    SELECT INSTITUTION_CD,
           ACAD_CAR_CD,
           TERM_CD,
           PERSON_ID,
           ENRL_DT,
           SRC_SYS_ID,
           TERM_SID,
           PERSON_SID,
           CLASS_ENRL_CNT_PREV_SUM,
           CLASS_ENRL_CNT,
           CLASS_ENRL_CNT_SUM,
           TAKEN_UNIT_PREV_SUM,
           TAKEN_UNIT,
           TAKEN_UNIT_SUM,
           ROW_NUMBER ()
               OVER (PARTITION BY INSTITUTION_CD,
                                  ACAD_CAR_CD,
                                  TERM_CD,
                                  PERSON_ID,
                                  SRC_SYS_ID
                     ORDER BY ENRL_DT)
               ENRL_FIRST_ORDER,
           ROW_NUMBER ()
               OVER (PARTITION BY INSTITUTION_CD,
                                  ACAD_CAR_CD,
                                  TERM_CD,
                                  PERSON_ID,
                                  SRC_SYS_ID
                     ORDER BY ENRL_DT DESC)
               ENRL_LAST_ORDER
      FROM E4
     WHERE (   (CLASS_ENRL_CNT_PREV_SUM <> CLASS_ENRL_CNT_SUM)
            OR (TAKEN_UNIT_PREV_SUM <> TAKEN_UNIT_SUM))
/
