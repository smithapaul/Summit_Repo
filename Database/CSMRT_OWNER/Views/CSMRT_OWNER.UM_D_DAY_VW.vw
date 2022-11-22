DROP VIEW CSMRT_OWNER.UM_D_DAY_VW
/

--
-- UM_D_DAY_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_DAY_VW
BEQUEATH DEFINER
AS 
with TERM as (
select distinct
       INSTITUTION_CD, TERM_CD, SRC_SYS_ID,
       min(FIRST_ENRLMT_DT) over (partition by INSTITUTION_CD, TERM_CD, SRC_SYS_ID) MIN_FIRST_ENRLMT_DT,
       max(ADD_DROP_END_DT) over (partition by INSTITUTION_CD, TERM_CD, SRC_SYS_ID) MAX_ADD_DROP_END_DT
  from PS_D_SESSION
 where DATA_ORIGIN <> 'D'
   and substr(TERM_CD, 3, 2) in ('10', '30')
   and FIRST_ENRLMT_DT >= '01-JAN-1950'  
),
        DAY
        AS (SELECT DAY_SID,
                   DAY_DT,
                   DAY_NUM,
                   DAY_DESCR,
                   DAY_ABBR,
                   DAY_JULIAN,
                   DAY_JULIAN_YYY,
                   DAY_WK_NUM,
                   DAY_MTH_NUM,
                   DAY_YR_NUM,
                   WEEK_SID,
                   WEEK_NUM,
                   WEEK_DESCR,
                   WEEK_MTH_NUM,
                   WEEK_YR_NUM,
                   MONTH_SID,
                   MONTH_NUM,
                   MONTH_DESCR,
                   MONTH_ABBR,
                   MONTH_QTR_NUM,
                   MONTH_YR_NUM,
                   QUARTER_SID,
                   QUARTER_NUM,
                   QUARTER_DESCR,
                   QUARTER_ABBR,
                   QUARTER_YR_NUM,
                   YEAR_SID,
                   YEAR_NUM,
                   YEAR_DESCR,
                   FIRSTDAYWK_FLG,
                   LASTDAYWK_FLG,
                   FIRSTDAYMTH_FLG,
                   LASTDAYMTH_FLG,
                   FIRSTDAYQTR_FLG,
                   LASTDAYQTR_FLG,
                   FIRSTDAYYR_FLG,
                   LASTDAYYR_FLG,
                   DAY_WEEKEND_FLG,
                   LOAD_ERROR,
                   DATA_ORIGIN,
                   CREATED_EW_DTTM,
                   LASTUPD_EW_DTTM,
                   BATCH_SID,
                   (SELECT max(S.TERM_CD)
                      FROM TERM S
                     WHERE S.INSTITUTION_CD = 'UMBOS'
                       AND S.MIN_FIRST_ENRLMT_DT < D.DAY_DT) UMBOS_CUR_TERM_CD,
                   (SELECT max(S.TERM_CD)
                      FROM TERM S
                     WHERE S.INSTITUTION_CD = 'UMDAR'
                       AND S.MIN_FIRST_ENRLMT_DT < D.DAY_DT) UMDAR_CUR_TERM_CD,
                   (SELECT max(S.TERM_CD)
                      FROM TERM S
                     WHERE S.INSTITUTION_CD = 'UMLOW'
                       AND S.MIN_FIRST_ENRLMT_DT < D.DAY_DT) UMLOW_CUR_TERM_CD,
                   (SELECT min(S.TERM_CD)
                      FROM TERM S
                     WHERE S.INSTITUTION_CD = 'UMBOS'
                       AND S.MAX_ADD_DROP_END_DT >= D.DAY_DT) UMBOS_CUR_TERM_CD2,
                   (SELECT min(S.TERM_CD)
                      FROM TERM S
                     WHERE S.INSTITUTION_CD = 'UMDAR'
                       AND S.MAX_ADD_DROP_END_DT >= D.DAY_DT) UMDAR_CUR_TERM_CD2,
                   (SELECT min(S.TERM_CD)
                      FROM TERM S
                     WHERE S.INSTITUTION_CD = 'UMLOW'
                       AND S.MAX_ADD_DROP_END_DT >= D.DAY_DT) UMLOW_CUR_TERM_CD2
              FROM PS_D_DAY D)
   SELECT DAY_SID,
          DAY_DT,
          DAY_NUM,
          DAY_DESCR,
          DAY_ABBR,
          DAY_JULIAN,
          DAY_JULIAN_YYY,
          DAY_WK_NUM,
          DAY_MTH_NUM,
          DAY_YR_NUM,
          WEEK_SID,
          WEEK_NUM,
          WEEK_DESCR,
          WEEK_MTH_NUM,
          WEEK_YR_NUM,
          MONTH_SID,
          MONTH_NUM,
          MONTH_DESCR,
          MONTH_ABBR,
          MONTH_QTR_NUM,
          MONTH_YR_NUM,
          QUARTER_SID,
          QUARTER_NUM,
          QUARTER_DESCR,
          QUARTER_ABBR,
          QUARTER_YR_NUM,
          YEAR_SID,
          YEAR_NUM,
          YEAR_DESCR,
          FIRSTDAYWK_FLG,
          LASTDAYWK_FLG,
          FIRSTDAYMTH_FLG,
          LASTDAYMTH_FLG,
          FIRSTDAYQTR_FLG,
          LASTDAYQTR_FLG,
          FIRSTDAYYR_FLG,
          LASTDAYYR_FLG,
          DAY_WEEKEND_FLG,
          UMBOS_CUR_TERM_CD,
          (SELECT TERM_CD_DESC
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMBOS'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD = DAY.UMBOS_CUR_TERM_CD)
             UMBOS_CUR_TERM_CD_DESC,
          (SELECT TERM_BEGIN_DT
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMBOS'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD = DAY.UMBOS_CUR_TERM_CD)
             UMBOS_CUR_TERM_BEGIN_DT,
          CAST ( (UMBOS_CUR_TERM_CD - 100) AS VARCHAR2 (4))
             UMBOS_PREV_TERM_CD,
          (SELECT T.TERM_CD_DESC
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMBOS'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD =
                         TO_CHAR (SUBSTR (DAY.UMBOS_CUR_TERM_CD, 1, 4) - 100))
             UMBOS_PREV_TERM_CD_DESC,
          (SELECT TERM_BEGIN_DT
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMBOS'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD =
                         TO_CHAR (DAY.UMBOS_CUR_TERM_CD - 100))
             UMBOS_PREV_TERM_BEGIN_DT,
          UMDAR_CUR_TERM_CD,
          (SELECT TERM_CD_DESC
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMDAR'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD = DAY.UMDAR_CUR_TERM_CD)
             UMDAR_CUR_TERM_CD_DESC,
          (SELECT TERM_BEGIN_DT
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMDAR'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD = DAY.UMDAR_CUR_TERM_CD)
             UMDAR_CUR_TERM_BEGIN_DT,
          CAST ( (UMDAR_CUR_TERM_CD - 100) AS VARCHAR2 (4))
             UMDAR_PREV_TERM_CD,
          (SELECT T.TERM_CD_DESC
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMDAR'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD =
                         TO_CHAR (DAY.UMDAR_CUR_TERM_CD - 100))
             UMDAR_PREV_TERM_CD_DESC,
          (SELECT TERM_BEGIN_DT
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMDAR'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD =
                         TO_CHAR (DAY.UMDAR_CUR_TERM_CD - 100))
             UMDAR_PREV_TERM_BEGIN_DT,
          UMLOW_CUR_TERM_CD,
          (SELECT TERM_CD_DESC
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMLOW'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD = DAY.UMLOW_CUR_TERM_CD)
             UMLOW_CUR_TERM_CD_DESC,
          (SELECT TERM_BEGIN_DT
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMLOW'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD = DAY.UMLOW_CUR_TERM_CD)
             UMLOW_CUR_TERM_BEGIN_DT,
          CAST ( (UMLOW_CUR_TERM_CD - 100) AS VARCHAR2 (4))
             UMLOW_PREV_TERM_CD,
          (SELECT T.TERM_CD_DESC
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMLOW'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD =
                         TO_CHAR (DAY.UMLOW_CUR_TERM_CD - 100))
             UMLOW_PREV_TERM_CD_DESC,
          (SELECT TERM_BEGIN_DT
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMLOW'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD =
                         TO_CHAR (DAY.UMLOW_CUR_TERM_CD - 100))
             UMLOW_PREV_TERM_BEGIN_DT,
          UMBOS_CUR_TERM_CD2,
          (SELECT TERM_CD_DESC
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMBOS'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD = DAY.UMBOS_CUR_TERM_CD2)
             UMBOS_CUR_TERM_CD_DESC2,
          (SELECT TERM_BEGIN_DT
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMBOS'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD = DAY.UMBOS_CUR_TERM_CD2)
             UMBOS_CUR_TERM_BEGIN_DT2,
          CAST ( (UMBOS_CUR_TERM_CD2 - 100) AS VARCHAR2 (4))
             UMBOS_PREV_TERM_CD2,
          (SELECT T.TERM_CD_DESC
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMBOS'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD =
                         TO_CHAR (
                            DAY.UMBOS_CUR_TERM_CD2 - 100))
             UMBOS_PREV_TERM_CD_DESC2,
          (SELECT TERM_BEGIN_DT
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMBOS'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD =
                         TO_CHAR (
                            DAY.UMBOS_CUR_TERM_CD2 - 100))
             UMBOS_PREV_TERM_BEGIN_DT2,
          UMDAR_CUR_TERM_CD2,
          (SELECT TERM_CD_DESC
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMDAR'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD = DAY.UMDAR_CUR_TERM_CD2)
             UMDAR_CUR_TERM_CD_DESC2,
          (SELECT TERM_BEGIN_DT
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMDAR'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD = DAY.UMDAR_CUR_TERM_CD2)
             UMDAR_CUR_TERM_BEGIN_DT2,
          CAST ( (UMDAR_CUR_TERM_CD2 - 100) AS VARCHAR2 (4))
             UMDAR_PREV_TERM_CD2,
          (SELECT T.TERM_CD_DESC
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMDAR'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD =
                         TO_CHAR (
                            DAY.UMDAR_CUR_TERM_CD2 - 100))
             UMDAR_PREV_TERM_CD_DESC2,
          (SELECT TERM_BEGIN_DT
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMDAR'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD =
                         TO_CHAR (
                            DAY.UMDAR_CUR_TERM_CD2 - 100))
             UMDAR_PREV_TERM_BEGIN_DT2,
          UMLOW_CUR_TERM_CD2 UMLOW_CUR_TERM_CD2,
          (SELECT TERM_CD_DESC
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMLOW'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD = DAY.UMLOW_CUR_TERM_CD2)
             UMLOW_CUR_TERM_CD_DESC2,
          (SELECT TERM_BEGIN_DT
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMLOW'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD = DAY.UMLOW_CUR_TERM_CD2)
             UMLOW_CUR_TERM_BEGIN_DT2,
          CAST ( (UMLOW_CUR_TERM_CD2 - 100) AS VARCHAR2 (4))
             UMLOW_PREV_TERM_CD2,
          (SELECT T.TERM_CD_DESC
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMLOW'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD =
                         TO_CHAR (
                            DAY.UMLOW_CUR_TERM_CD2 - 100))
             UMLOW_PREV_TERM_CD_DESC2,
          (SELECT TERM_BEGIN_DT
             FROM PS_D_TERM T
            WHERE     T.INSTITUTION_CD = 'UMLOW'
                  AND T.ACAD_CAR_CD = 'UGRD'
                  AND T.TERM_CD =
                         TO_CHAR (
                            DAY.UMLOW_CUR_TERM_CD2 - 100))
             UMLOW_PREV_TERM_BEGIN_DT2,
          LOAD_ERROR,
          DATA_ORIGIN,
          CREATED_EW_DTTM,
          LASTUPD_EW_DTTM,
          BATCH_SID
     FROM DAY
/
