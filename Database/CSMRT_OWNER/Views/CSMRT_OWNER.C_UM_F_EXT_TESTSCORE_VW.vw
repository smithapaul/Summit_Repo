DROP VIEW CSMRT_OWNER.C_UM_F_EXT_TESTSCORE_VW
/

--
-- C_UM_F_EXT_TESTSCORE_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_UM_F_EXT_TESTSCORE_VW
BEQUEATH DEFINER
AS 
SELECT PERSON_SID,
           EXT_TST_CMPNT_SID,
           TO_DATE (
               CASE
                   WHEN EXT_TST_DT = '01-JAN-1900' THEN NULL
                   ELSE EXT_TST_DT
               END)
               EXT_TST_DT,
           TST_DATA_SRC_SID,
           SRC_SYS_ID,
           PERSON_ID,
           TEST_ID,
           TEST_CMPNT_ID,
           TEST_DATA_SRC_ID,
           EXT_ACAD_LVL_SID,
           NUMERIC_SCORE,
           LETTER_SCORE,
           SCORE_PERCENTILE,
           LOAD_DT,
           TEST_ADMIN,
           TEST_INDEX,
           MAX_SCORE,
           MIN_SCORE,
           (CASE
                WHEN NUMERIC_SCORE BETWEEN MIN_SCORE AND MAX_SCORE THEN 'Y'
                WHEN MIN_SCORE - MAX_SCORE = 0 THEN 'Y'
                ELSE 'N'
            END)
               VALID_TEST_SCORE_FLG,
           TEST_CMPNT_ORDER,
           TEST_DT_ORDER,
           TEST_SUM_ORDER,
           TEST_SOURCE_ORDER,
           BEST_SCORE_FLG,
           HIGHEST_SCORE_FLG,
           LATEST_TEST_DT_FLG,
           CONV_FLG
      FROM UM_F_EXT_TESTSCORE
     WHERE ROWNUM < 1000000000
/
