CREATE OR REPLACE VIEW UM_D_PRSPCT_CAR_VW
BEQUEATH DEFINER
AS 
WITH
        ATHL
        AS
            (  SELECT C1.INSTITUTION_SID,
                      C1.PERSON_SID,
                      MIN (A1.PERSON_SID)     PERSON_ATHL_SID
                 FROM UM_D_PERSON_ATHL A1, UM_D_PRSPCT_CAR C1
                WHERE     A1.PERSON_SID = C1.PERSON_SID
                      AND SUBSTR (A1.SPORT, 1, 1) =
                          SUBSTR (C1.INSTITUTION_CD, 3, 1)
                      AND A1.DATA_ORIGIN <> 'D'
             GROUP BY C1.INSTITUTION_SID, C1.PERSON_SID),
        SRVC
        AS
            (  SELECT C1.INSTITUTION_SID,
                      C1.PERSON_SID,
                      MIN (S1.PERSON_SID)     PERSON_SRVC_IND_SID
                 --                 FROM UM_R_SRVC_IND S1, UM_D_PRSPCT_CAR C1
                 FROM UM_D_PERSON_SRVC_IND S1, UM_D_PRSPCT_CAR C1  -- Mar 2019
                WHERE     S1.PERSON_SID = C1.PERSON_SID
                      AND S1.INSTITUTION_SID = C1.INSTITUTION_SID
                      AND S1.DATA_ORIGIN <> 'D'
             GROUP BY C1.INSTITUTION_SID, C1.PERSON_SID)
    SELECT PRSPCT_CAR_SID,
           C.INSTITUTION_CD,
           C.ACAD_CAR_CD,
           ADMIT_TERM,
           EMPLID,
           C.SRC_SYS_ID,
           C.ACAD_CAR_SID,
           ACAD_LOAD_SID,
           ACAD_LVL_SID,
           ADM_CREATION_DT,
           ADM_RECR_CTR,
           ADMIT_TERM_SID,
           ADMIT_TYPE_SID,
           APPL_ON_FILE,
           CAMPUS_SID,
           FIN_AID_INTEREST,
           HOUSING_INTEREST,
           NVL (
               (SELECT MIN (X.XLATSHORTNAME)
                  FROM UM_D_XLATITEM_VW X
                 WHERE     X.FIELDNAME = 'HOUSING_INTEREST'
                       AND X.FIELDVALUE = C.HOUSING_INTEREST),
               '-')
               HOUSING_INTEREST_SD,                         -- Added 1/19/2012
           NVL (
               (SELECT MIN (X.XLATLONGNAME)
                  FROM UM_D_XLATITEM_VW X
                 WHERE     X.FIELDNAME = 'HOUSING_INTEREST'
                       AND X.FIELDVALUE = C.HOUSING_INTEREST),
               '-')
               HOUSING_INTEREST_LD,                         -- Added 1/19/2012
           C.INSTITUTION_SID,
           LST_SCHL_ATTND_SID,
           LST_SCHL_GRDDT_SID,
           CASE
               WHEN isdate (TO_CHAR (LST_SCHL_GRDDT_SID), 'YYYYMMDD') = 0
               THEN
                   TO_DATE (LST_SCHL_GRDDT_SID, 'YYYYMMDD')
               ELSE
                   NULL
           END
               LST_SCHL_GRDDT_DT,                                  -- APR 2017
           --          NVL (
           --             (SELECT MIN (A.PERSON_SID)
           --                FROM UM_D_PERSON_ATHL A
           --               WHERE     A.PERSON_SID = C.PERSON_SID
           --                     AND SUBSTR (A.SPORT, 1, 1) =
           --                            SUBSTR (C.INSTITUTION_CD, 3, 1)
           --                     AND A.DATA_ORIGIN <> 'D'),
           --             2147483646)
           --             PERSON_ATHL_SID,
           NVL (ATHL.PERSON_ATHL_SID, 2147483646)
               PERSON_ATHL_SID,
           --          NVL (
           --             (SELECT MIN (S.PERSON_SID)
           --                FROM UM_R_SRVC_IND S
           --               WHERE     S.PERSON_SID = C.PERSON_SID
           --                     AND S.INSTITUTION = C.INSTITUTION_CD
           --                     AND S.DATA_ORIGIN <> 'D'),
           --             2147483646)
           --             PERSON_SRVC_IND_SID,
           NVL (SRVC.PERSON_SRVC_IND_SID, 2147483646)
               PERSON_SRVC_IND_SID,
           C.PERSON_SID,
           RECRT_CNTR_SID,
           RECRT_STAT_SID,
           TO_DATE (RECRT_STAT_DT_SID, 'YYYYMMDD')
               RECRT_STAT_DT,
           RECRT_STAT_DT_SID,
           RECRTR_SID,
           REGION_CS_SID,
           REGION_FROM,
           NVL (R.RSDNCY_SID, 2147483646)
               RSDNCY_SID,
           RFRL_SRC_SID,
           TO_DATE (RFRL_SRC_DT_SID, 'YYYYMMDD')
               RFRL_SRC_DT,
           RFRL_SRC_DT_SID,
           (SELECT MAX (CLASS_RANK)
              FROM PS_F_EXT_ACAD_SUMM S
             WHERE     C.PERSON_SID = S.PERSON_SID
                   AND C.SRC_SYS_ID = S.SRC_SYS_ID
                   AND C.LST_SCHL_ATTND_SID = S.EXT_ORG_SID
                   AND C.INSTITUTION_SID = S.INSTITUTION_SID)
               CLASS_RANK,                                    -- Added 2/13/12
           (SELECT MAX (CLASS_SIZE)
              FROM PS_F_EXT_ACAD_SUMM S
             WHERE     C.PERSON_SID = S.PERSON_SID
                   AND C.SRC_SYS_ID = S.SRC_SYS_ID
                   AND C.LST_SCHL_ATTND_SID = S.EXT_ORG_SID
                   AND C.INSTITUTION_SID = S.INSTITUTION_SID)
               CLASS_SIZE,                                    -- Added 2/13/12
           (SELECT MAX (NUMERIC_SCORE)
              FROM UM_F_EXT_TESTSCORE T, PS_D_EXT_TST_CMPNT T2
             WHERE     T.EXT_TST_CMPNT_SID = T2.EXT_TST_CMPNT_SID
                   AND T.TEST_CMPNT_ORDER = 1
                   AND T.PERSON_SID = C.PERSON_SID
                   AND T2.EXT_TST_ID = 'ACT'
                   AND T2.EXT_TST_CMPNT_ID = 'COMP')
               ACT_COMP_SCORE,                                -- Added 2/13/12
           (SELECT MAX (NUMERIC_SCORE)
              FROM UM_F_EXT_TESTSCORE T, PS_D_EXT_TST_CMPNT T2
             WHERE     T.EXT_TST_CMPNT_SID = T2.EXT_TST_CMPNT_SID
                   AND T.TEST_DT_ORDER = 1
                   AND T.PERSON_SID = C.PERSON_SID
                   AND T2.EXT_TST_ID = 'GMAT'
                   AND T2.EXT_TST_CMPNT_ID = 'TOTAL')
               GMAT_TOTAL_SCORE,                              -- Added 2/13/12
           (SELECT MAX (NUMERIC_SCORE)
              FROM UM_F_EXT_TESTSCORE T, PS_D_EXT_TST_CMPNT T2
             WHERE     T.EXT_TST_CMPNT_SID = T2.EXT_TST_CMPNT_SID
                   AND T.TEST_DT_ORDER = 1
                   AND T.PERSON_SID = C.PERSON_SID
                   AND T2.EXT_TST_ID = 'GRE'
                   AND T2.EXT_TST_CMPNT_ID = 'QUANT')
               GRE_QUAN_SCORE,                                -- Added 2/13/12
           (SELECT MAX (NUMERIC_SCORE)
              FROM UM_F_EXT_TESTSCORE T, PS_D_EXT_TST_CMPNT T2
             WHERE     T.EXT_TST_CMPNT_SID = T2.EXT_TST_CMPNT_SID
                   AND T.TEST_DT_ORDER = 1
                   AND T.PERSON_SID = C.PERSON_SID
                   AND T2.EXT_TST_ID = 'GRE'
                   AND T2.EXT_TST_CMPNT_ID = 'VERBR')
               GRE_VERB_SCORE,                                -- Added 2/13/12
           (SELECT MAX (NUMERIC_SCORE)
              FROM UM_F_EXT_TESTSCORE T, PS_D_EXT_TST_CMPNT T2
             WHERE     T.EXT_TST_CMPNT_SID = T2.EXT_TST_CMPNT_SID
                   AND T.TEST_DT_ORDER = 1
                   AND T.PERSON_SID = C.PERSON_SID
                   AND T2.EXT_TST_ID = 'GRE'
                   AND T2.EXT_TST_CMPNT_ID = 'WR')
               GRE_ANLY_SCORE,                                -- Added 2/13/12
           (SELECT MAX (NUMERIC_SCORE)
              FROM UM_F_EXT_TESTSCORE T, PS_D_EXT_TST_CMPNT T2
             WHERE     T.EXT_TST_CMPNT_SID = T2.EXT_TST_CMPNT_SID
                   AND T.TEST_DT_ORDER = 1
                   AND T.PERSON_SID = C.PERSON_SID
                   AND T2.EXT_TST_ID = 'LSAT'
                   AND T2.EXT_TST_CMPNT_ID = 'COMP')
               LSAT_COMP_SCORE,                               -- Added 2/13/12
           (SELECT MAX (NUMERIC_SCORE)
              FROM UM_F_EXT_TESTSCORE T, PS_D_EXT_TST_CMPNT T2
             WHERE     T.EXT_TST_CMPNT_SID = T2.EXT_TST_CMPNT_SID
                   AND T.TEST_CMPNT_ORDER = 1
                   AND T.PERSON_SID = C.PERSON_SID
                   AND T2.EXT_TST_ID = 'SAT I'
                   AND T2.EXT_TST_CMPNT_ID = 'MATH')
               SAT_MATH_SCORE,                                -- Added 2/13/12
           (SELECT MAX (NUMERIC_SCORE)
              FROM UM_F_EXT_TESTSCORE T, PS_D_EXT_TST_CMPNT T2
             WHERE     T.EXT_TST_CMPNT_SID = T2.EXT_TST_CMPNT_SID
                   AND T.TEST_CMPNT_ORDER = 1
                   AND T.PERSON_SID = C.PERSON_SID
                   AND T2.EXT_TST_ID = 'SAT I'
                   AND T2.EXT_TST_CMPNT_ID = 'VERB')
               SAT_VERB_SCORE,                                -- Added 2/13/12
           C.LOAD_ERROR,
           C.DATA_ORIGIN,
           C.CREATED_EW_DTTM,
           C.LASTUPD_EW_DTTM,
           C.BATCH_SID
      FROM UM_D_PRSPCT_CAR  C
           LEFT OUTER JOIN UM_R_PERSON_RSDNCY R
               ON     C.PERSON_SID = R.PERSON_SID
                  AND C.INSTITUTION_SID = R.INSTITUTION_SID
                  AND C.ACAD_CAR_SID = R.ACAD_CAR_SID
                  AND C.ADMIT_TERM_SID = R.EFF_TERM_SID
           LEFT OUTER JOIN ATHL
               ON     C.INSTITUTION_SID = ATHL.INSTITUTION_SID
                  AND C.PERSON_SID = ATHL.PERSON_SID
           LEFT OUTER JOIN SRVC
               ON     C.INSTITUTION_SID = SRVC.INSTITUTION_SID
                  AND C.PERSON_SID = SRVC.PERSON_SID;
