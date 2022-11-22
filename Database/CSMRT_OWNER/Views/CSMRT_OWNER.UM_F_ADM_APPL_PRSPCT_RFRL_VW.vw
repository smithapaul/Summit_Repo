DROP VIEW CSMRT_OWNER.UM_F_ADM_APPL_PRSPCT_RFRL_VW
/

--
-- UM_F_ADM_APPL_PRSPCT_RFRL_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_ADM_APPL_PRSPCT_RFRL_VW
BEQUEATH DEFINER
AS 
SELECT A.ADM_APPL_SID,
           NVL (C.PRSPCT_CAR_SID, 2147483646)                  PRSPCT_CAR_SID,
           NVL (F.RFRL_DTL_SID, 2147483646)                    RFRL_DTL_SID,
           A.SRC_SYS_ID,
           NVL (A.INSTITUTION_CD, '-')                         INSTITUTION_CD,
           NVL (C.ACAD_CAR_CD, '-')                            ACAD_CAR_CD,
           NVL (C.ADMIT_TERM, '-')                             ADMIT_TERM,
           NVL (C.EMPLID, '-')                                 EMPLID,
           NVL (F.RFRL_GRP, '-')                               RFRL_GRP,
           NVL (F.RFRL_DTL, '-')                               RFRL_DTL,
           NVL (F.ADM_RECR_CTR, '-')                           ADM_RECR_CTR,
           NVL (A.INSTITUTION_SID, 2147483646)                 INSTITUTION_SID,
           NVL (A.ACAD_CAR_SID, 2147483646)                    ACAD_CAR_SID,
           NVL (A.ADMIT_TERM_SID, 2147483646)                  ADMIT_TERM_SID,
           NVL (A.APPLCNT_SID, 2147483646)                     PERSON_SID,
           NVL (F.RECRT_CNTR_SID, 2147483646)                  RECRT_CNTR_SID,
--           TO_DATE (NVL (F.RFRL_DT_SID, 19000101), 'YYYYMMDD') RFRL_DT,
           nvl(F.RFRL_DT, to_date('01-JAN-1900')) RFRL_DT,       -- Nov 2018 
--           NVL (F.RFRL_DT_SID, 19000101)                       RFRL_DT_SID,       -- Nov 2018 
           to_number(NVL(F.RFRL_DT,to_date('19000101','YYYYMMDD')),'YYYYMMDD') RFRL_DT_SID,
           DENSE_RANK ()
           OVER (PARTITION BY C.PRSPCT_CAR_SID, C.SRC_SYS_ID
--                 ORDER BY F.RFRL_DT_SID)
                 ORDER BY F.RFRL_DT)        -- Nov 2018 
               INIT_RFRL_ORDER,
           DENSE_RANK ()
           OVER (PARTITION BY C.PRSPCT_CAR_SID, C.SRC_SYS_ID
--                 ORDER BY F.RFRL_DT_SID DESC)
                 ORDER BY F.RFRL_DT DESC)       -- Nov 2018 
               LAST_RFRL_ORDER
      FROM UM_F_ADM_APPL_STAT  A
           LEFT OUTER JOIN UM_D_PRSPCT_CAR C
               ON     A.APPLCNT_SID = C.PERSON_SID
                  AND A.ADMIT_TERM_SID = C.ADMIT_TERM_SID
                  AND A.SRC_SYS_ID = C.SRC_SYS_ID
                  AND NVL (C.DATA_ORIGIN, '-') <> 'D'
           LEFT OUTER JOIN UM_F_PRSPCT_RFRL F
               ON     F.PRSPCT_CAR_SID = C.PRSPCT_CAR_SID
                  AND NVL (F.DATA_ORIGIN, '-') <> 'D'
     WHERE ROWNUM < 1000000000
/
