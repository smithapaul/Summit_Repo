CREATE OR REPLACE VIEW UM_D_PRSPCT_ADM_APPL_VW
BEQUEATH DEFINER
AS 
WITH ADM
        AS (SELECT A.ADM_APPL_SID,
                   A.APPLCNT_SID,
                   A.ADMIT_TERM_SID,
                   A.ACAD_PROG_SID,
                   A.ACAD_PLAN_SID,
                   A.SRC_SYS_ID,
                   ROW_NUMBER ()
                   OVER (
                      PARTITION BY A.APPLCNT_SID,
                                   A.ADMIT_TERM_SID,
                                   A.ACAD_PROG_SID,
                                   A.ACAD_PLAN_SID,
                                   A.SRC_SYS_ID
                      ORDER BY
                         DECODE (S.PROG_STAT_CD,
                                 'AC', 1,
                                 'PM', 2,
                                 'AD', 3,
                                 9),
                         A.APPL_CREATE_DT,
                         A.ADM_APPL_NBR DESC)
                      ADM_ORDER
              FROM UM_F_ADM_APPL_STAT A
                   JOIN PS_D_PROG_STAT S
                      ON     A.PROG_STAT_SID = S.PROG_STAT_SID
                         AND A.APPL_COUNT_ORDER = 1)
   SELECT C.PRSPCT_CAR_SID,
          G.PRSPCT_PROG_SID,
          P.PRSPCT_PLAN_SID,
          C.SRC_SYS_ID,
          C.INSTITUTION_CD,
          C.ACAD_CAR_CD,
          C.ADMIT_TERM,
          C.EMPLID,
          G.ACAD_PROG_CD,
          P.ACAD_PLAN_CD,
          NVL (ADM.ADM_APPL_SID, 2147483646) ADM_APPL_SID
     FROM UM_D_PRSPCT_CAR C
          JOIN UM_D_PRSPCT_PROG G ON C.PRSPCT_CAR_SID = G.PRSPCT_CAR_SID
          JOIN UM_D_PRSPCT_PLAN P ON G.PRSPCT_PROG_SID = P.PRSPCT_PROG_SID
          LEFT OUTER JOIN ADM
             ON     C.ADMIT_TERM_SID = ADM.ADMIT_TERM_SID
                AND C.PERSON_SID = ADM.APPLCNT_SID
                AND G.ACAD_PROG_SID = ADM.ACAD_PROG_SID
                AND P.ACAD_PLAN_SID = ADM.ACAD_PLAN_SID
                AND C.SRC_SYS_ID = ADM.SRC_SYS_ID
                AND ADM.ADM_ORDER = 1;
