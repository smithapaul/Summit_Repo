DROP VIEW CSMRT_OWNER.UM_F_PRSPCT_RFRL_VW
/

--
-- UM_F_PRSPCT_RFRL_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_PRSPCT_RFRL_VW
BEQUEATH DEFINER
AS 
SELECT C.PRSPCT_CAR_SID,
           NVL (G.PRSPCT_PROG_SID, 2147483646)
               PRSPCT_PROG_SID,
           NVL (L.PRSPCT_PLAN_SID, 2147483646)
               PRSPCT_PLAN_SID,
           NVL (S.PRSPCT_SPLAN_SID, 2147483646)
               PRSPCT_SPLAN_SID,
           NVL (F.RECRT_CNTR_SID, 2147483646)
               RECRT_CNTR_SID,
           NVL (F.RFRL_DTL_SID, 2147483646)
               RFRL_DTL_SID,
           --           TO_DATE (NVL (F.RFRL_DT_SID, 19000101), 'YYYYMMDD') RFRL_DT,
           NVL (F.RFRL_DT, TO_DATE ('01-JAN-1900'))
               RFRL_DT,                                            -- Nov 2018
           --           NVL (F.RFRL_DT_SID, 19000101)                       RFRL_DT_SID,       -- Nov 2018
           TO_NUMBER (
               TO_CHAR (NVL (F.RFRL_DT, TO_DATE ('19000101', 'YYYYMMDD')),
                        'YYYYMMDD'))
               RFRL_DT_SID,
           C.SRC_SYS_ID,
           --C.ACAD_CAR_SID,
           --G.ACAD_PROG_SID,
           --L.ACAD_PLAN_SID,
           --S.ACAD_SPLAN_SID,
           --C.ADMIT_TERM_SID,
           C.INSTITUTION_CD,
           C.INSTITUTION_SID,
           C.PERSON_SID,
           NVL (R.RSDNCY_SID, 2147483646)
               RSDNCY_SID,
           NVL (R.ADM_RSDNCY_SID, 2147483646)
               ADM_RSDNCY_SID,
           NVL (R.FA_FED_RSDNCY_SID, 2147483646)
               FA_FED_RSDNCY_SID,
           NVL (R.FA_ST_RSDNCY_SID, 2147483646)
               FA_ST_RSDNCY_SID,
           NVL (R.TUITION_RSDNCY_SID, 2147483646)
               TUITION_RSDNCY_SID,
           CAST (NVL (F.ADMIT_TERM, '-') AS VARCHAR2 (4))
               ADMIT_TERM,                                         -- Nov 2017
           CAST (NVL (F.ADM_RECR_CTR, '-') AS VARCHAR2 (4))
               ADM_RECR_CTR,                                       -- Nov 2017
           CASE
               WHEN F.RFRL_DTL = 'WWW'
               THEN
                   CAST (NVL (F.UM_ADM_REC_NBR, '-') AS VARCHAR2 (15))
               ELSE
                   '-'
           END
               UM_ADM_REC_NBR,                                     -- Nov 2017
           (CASE
                WHEN G.ACAD_PROG_CD IS NULL
                THEN
                    'U'
                WHEN EXISTS
                         (SELECT 1
                            FROM UM_F_ADM_APPL_STAT S1
                           WHERE     C.INSTITUTION_SID = S1.INSTITUTION_SID
                                 AND C.ACAD_CAR_SID = S1.ACAD_CAR_SID
                                 AND G.ACAD_PROG_SID = S1.ACAD_PROG_SID
                                 AND C.ADMIT_TERM_SID = S1.ADMIT_TERM_SID
                                 AND C.PERSON_SID = S1.APPLCNT_SID
                                 AND S1.APPL_COUNT_ORDER = 1)
                THEN
                    'Y'
                ELSE
                    'N'
            END)
               APPL_PROG_FLAG,
           ROW_NUMBER ()
               OVER (PARTITION BY C.PRSPCT_CAR_SID,
                                  G.PRSPCT_PROG_SID,
                                  L.PRSPCT_PLAN_SID,
                                  S.PRSPCT_SPLAN_SID,
                                  C.SRC_SYS_ID
                     --                   ORDER BY F.RFRL_DT_SID, F.RFRL_DTL_SID, F.RECRT_CNTR_SID)
                     ORDER BY F.RFRL_DT, F.RFRL_DTL_SID, F.RECRT_CNTR_SID)
               INIT_RFRL_ORDER,
           ROW_NUMBER ()
               OVER (
                   PARTITION BY C.PRSPCT_CAR_SID,
                                G.PRSPCT_PROG_SID,
                                L.PRSPCT_PLAN_SID,
                                S.PRSPCT_SPLAN_SID,
                                C.SRC_SYS_ID
                   ORDER BY --                       F.RFRL_DT_SID DESC, F.RFRL_DTL_SID, F.RECRT_CNTR_SID)
                            F.RFRL_DT DESC, F.RFRL_DTL_SID, F.RECRT_CNTR_SID)
               LAST_RFRL_ORDER,
           1
               PRSPCT_CNT,
           --nvl(S2.APPL_CNT,0) APPL_CNT,
           NVL (
               (SELECT MAX (APPL_CNT)
                  FROM UM_F_ADM_APPL_STAT S2
                 WHERE     S2.APPL_COUNT_ORDER = 1
                       AND C.INSTITUTION_SID = S2.INSTITUTION_SID
                       AND C.ACAD_CAR_SID = S2.ACAD_CAR_SID
                       AND C.ADMIT_TERM_SID = S2.ADMIT_TERM_SID
                       AND C.PERSON_SID = S2.APPLCNT_SID),
               0)
               APPL_CNT,
           NVL (
               (SELECT MAX (APPL_COMPLETE_CNT)
                  FROM UM_F_ADM_APPL_STAT S2
                 WHERE     S2.APPL_COUNT_ORDER = 1
                       AND C.INSTITUTION_SID = S2.INSTITUTION_SID
                       AND C.ACAD_CAR_SID = S2.ACAD_CAR_SID
                       AND C.ADMIT_TERM_SID = S2.ADMIT_TERM_SID
                       AND C.PERSON_SID = S2.APPLCNT_SID),
               0)
               APPL_COMPLETE_CNT,
           NVL (
               (SELECT MAX (ADMIT_CNT)
                  FROM UM_F_ADM_APPL_STAT S2
                 WHERE     S2.APPL_COUNT_ORDER = 1
                       AND C.INSTITUTION_SID = S2.INSTITUTION_SID
                       AND C.ACAD_CAR_SID = S2.ACAD_CAR_SID
                       AND C.ADMIT_TERM_SID = S2.ADMIT_TERM_SID
                       AND C.PERSON_SID = S2.APPLCNT_SID),
               0)
               ADMIT_CNT,
           NVL (
               (SELECT MAX (DENY_CNT)
                  FROM UM_F_ADM_APPL_STAT S2
                 WHERE     S2.APPL_COUNT_ORDER = 1
                       AND C.INSTITUTION_SID = S2.INSTITUTION_SID
                       AND C.ACAD_CAR_SID = S2.ACAD_CAR_SID
                       AND C.ADMIT_TERM_SID = S2.ADMIT_TERM_SID
                       AND C.PERSON_SID = S2.APPLCNT_SID),
               0)
               DENY_CNT,
           NVL (
               (SELECT MAX (DEPOSIT_CNT)
                  FROM UM_F_ADM_APPL_STAT S2
                 WHERE     S2.APPL_COUNT_ORDER = 1
                       AND C.INSTITUTION_SID = S2.INSTITUTION_SID
                       AND C.ACAD_CAR_SID = S2.ACAD_CAR_SID
                       AND C.ADMIT_TERM_SID = S2.ADMIT_TERM_SID
                       AND C.PERSON_SID = S2.APPLCNT_SID),
               0)
               DEPOSIT_CNT,
           NVL (
               (SELECT MAX (ENROLL_CNT)
                  FROM UM_F_ADM_APPL_STAT S2
                 WHERE     S2.APPL_COUNT_ORDER = 1
                       AND C.INSTITUTION_SID = S2.INSTITUTION_SID
                       AND C.ACAD_CAR_SID = S2.ACAD_CAR_SID
                       AND C.ADMIT_TERM_SID = S2.ADMIT_TERM_SID
                       AND C.PERSON_SID = S2.APPLCNT_SID),
               0)
               ENROLL_CNT
      FROM UM_D_PRSPCT_CAR  C                                       -- Swapped
           LEFT OUTER JOIN UM_R_PERSON_RSDNCY R
               ON     C.INSTITUTION_SID = R.INSTITUTION_SID
                  AND C.ACAD_CAR_SID = R.ACAD_CAR_SID
                  AND C.ADMIT_TERM_SID = R.EFF_TERM_SID
                  AND C.PERSON_SID = R.PERSON_SID
                  AND C.SRC_SYS_ID = R.SRC_SYS_ID
           LEFT OUTER JOIN UM_F_PRSPCT_RFRL F
               ON     F.PRSPCT_CAR_SID = C.PRSPCT_CAR_SID
                  AND NVL (F.DATA_ORIGIN, '-') <> 'D'
           LEFT OUTER JOIN UM_D_PRSPCT_PROG G
               ON     G.PRSPCT_CAR_SID = C.PRSPCT_CAR_SID
                  AND NVL (G.DATA_ORIGIN, '-') <> 'D'
           LEFT OUTER JOIN UM_D_PRSPCT_PLAN L
               ON     L.PRSPCT_PROG_SID = G.PRSPCT_PROG_SID
                  AND NVL (L.DATA_ORIGIN, '-') <> 'D'
           LEFT OUTER JOIN UM_D_PRSPCT_SBPLAN S
               ON     S.PRSPCT_PLAN_SID = L.PRSPCT_PLAN_SID
                  AND NVL (S.DATA_ORIGIN, '-') <> 'D'
     WHERE C.DATA_ORIGIN <> 'D' AND ROWNUM < 1000000000
/
