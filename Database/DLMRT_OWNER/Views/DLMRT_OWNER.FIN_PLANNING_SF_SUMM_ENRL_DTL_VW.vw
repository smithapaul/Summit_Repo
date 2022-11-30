DROP VIEW DLMRT_OWNER.FIN_PLANNING_SF_SUMM_ENRL_DTL_VW
/

--
-- FIN_PLANNING_SF_SUMM_ENRL_DTL_VW  (View) 
--
CREATE OR REPLACE VIEW DLMRT_OWNER.FIN_PLANNING_SF_SUMM_ENRL_DTL_VW
BEQUEATH DEFINER
AS 
WITH
        SF_SUMM
        AS
            (  SELECT /*+PARALLEL(8) inline no_merge USE_HASH(T0)*/
                      T0.INSTITUTION_CD,
                      T0.ACAD_CAR_CD,
                      T0.PERSON_ID,
                      T0.FISCAL_YEAR,
                      T0.ITEM_TERM,
                      T0.ITEM_TERM_LD,
                      SUM (CASE
                               WHEN (SF_CATEGORY_CALC IN ('Fees',
                                                          'Grants',
                                                          'Scholarships',
                                                          'Tuition',
                                                          'Waivers'))
                               THEN
                                   (MONETARY_AMOUNT)
                               ELSE
                                   0
                           END)    AS NET_AMOUNT,
                      SUM (
                          CASE
                              WHEN (SF_CATEGORY_CALC IN ('Fees', 'Tuition'))
                              THEN
                                  (MONETARY_AMOUNT)
                              ELSE
                                  0
                          END)     AS GROSS_AMOUNT,
                      SUM (
                          CASE
                              WHEN (SF_CATEGORY_CALC IN
                                        ('Grants', 'Scholarships', 'Waivers'))
                              THEN
                                  (MONETARY_AMOUNT)
                              ELSE
                                  0
                          END)     AS AID_AMOUNT,
                      SUM (
                          CASE
                              WHEN (SF_CATEGORY_CALC IN ('Grants'))
                              THEN
                                  (MONETARY_AMOUNT)
                              ELSE
                                  0
                          END)     AS GRANT_AMOUNT,
                      SUM (
                          CASE
                              WHEN (SF_CATEGORY_CALC IN ('Scholarships'))
                              THEN
                                  (MONETARY_AMOUNT)
                              ELSE
                                  0
                          END)     AS SCHOLARSHIP_AMOUNT,
                      SUM (
                          CASE
                              WHEN (SF_CATEGORY_CALC IN ('Waivers'))
                              THEN
                                  (MONETARY_AMOUNT)
                              ELSE
                                  0
                          END)     AS WAIVER_AMOUNT,
                      SUM (
                          CASE
                              WHEN (FA_SOURCE_CALC IN ('Federal') AND SF_CATEGORY_CALC IN ('Fees', 'Grants', 'Scholarships', 'Tuition', 'Waivers'))
                              THEN
                                  (MONETARY_AMOUNT)
                              ELSE
                                  0
                          END)     AS FEDERAL_AID_AMOUNT,
                      SUM (
                          CASE
                              WHEN (FA_SOURCE_CALC IN ('State') AND SF_CATEGORY_CALC IN ('Fees', 'Grants', 'Scholarships', 'Tuition', 'Waivers'))
                              THEN
                                  (MONETARY_AMOUNT)
                              ELSE
                                  0
                          END)     AS STATE_AID_AMOUNT,
                      SUM (
                          CASE
                              WHEN (FA_SOURCE_CALC IN ('Institutional') AND SF_CATEGORY_CALC IN ('Fees', 'Grants', 'Scholarships', 'Tuition', 'Waivers'))
                              THEN
                                  (MONETARY_AMOUNT)
                              ELSE
                                  0
                          END)     AS INST_AID_AMOUNT,
                      SUM (
                          CASE
                              WHEN (FA_SOURCE_CALC IN ('Private') AND SF_CATEGORY_CALC IN ('Fees', 'Grants', 'Scholarships', 'Tuition', 'Waivers'))
                              THEN
                                  (MONETARY_AMOUNT)
                              ELSE
                                  0
                          END)     AS PRIVATE_AID_AMOUNT
                 FROM DLMRT_OWNER.FIN_PLANNING_SF_DETAIL_VW T0
             GROUP BY T0.INSTITUTION_CD,
                      T0.ACAD_CAR_CD,
                      T0.PERSON_ID,
                      T0.FISCAL_YEAR,
                      T0.ITEM_TERM,
                      T0.ITEM_TERM_LD)
    SELECT /*+inline parallel(8) USE_HASH(FP_ENRL CLASSORG_FIN SF_SUMM)*/
           FP_ENRL.INSTITUTION_CD,
           FP_ENRL.TERM_CD,
           FP_ENRL.TERM_LD,
           TO_NUMBER (FP_ENRL.FISCAL_YEAR)
               AS FISCAL_YEAR,
           FP_ENRL.PERSON_ID,
           FP_ENRL.ACAD_CAR_CD,
           FP_ENRL.ACAD_CAR_LD,
           FP_ENRL.RSDNCY_LD,
           FP_ENRL.RSDNCY_ID,
           FP_ENRL.TOT_CREDITS,
           FP_ENRL.DAY_CREDITS,
           FP_ENRL.CE_CREDITS,
           FP_ENRL.ONLINE_CREDITS,
           FP_ENRL.DAY_ONLINE_CREDITS,
           FP_ENRL.CE_ONLINE_CREDITS,
           FP_ENRL.TOT_FTE,
           FP_ENRL.DAY_FTE,
           FP_ENRL.CE_FTE,
           FP_ENRL.TOT_FFTE,
           FP_ENRL.DAY_FFTE,
           FP_ENRL.CE_FFTE,
           FP_ENRL.ACAD_PROG_CD,
           FP_ENRL.ACAD_PROG_LD,
           FP_ENRL.PROG_GRP_CD,
           FP_ENRL.PROG_GRP_LD,
           FP_ENRL.PROG_ORG_CD,
           FP_ENRL.PROG_ORG_LD,
           FP_ENRL.ACAD_PLAN_CD,
           FP_ENRL.ACAD_PLAN_LD,
           FP_ENRL.PLAN_ORG_CD,
           FP_ENRL.PLAN_ORG_LD,
           FP_ENRL.PLAN_PERCENT_OWNED,
           FP_ENRL.DEGREE_SEEKING_FLG,
           --FP_ENRL.TAKEN_UNIT,
           --FP_ENRL.CLASS_FFTE,
           ROUND (
                 (FP_ENRL.TAKEN_UNIT)
               * NVL ((CLASSORG_FIN.PERCENT_OWNED / 100), 1),
               3)
               TAKEN_UNIT,
           ROUND (
               CASE
                   WHEN FP_ENRL.TOT_CREDITS >= 12
                   THEN
                         (  (FP_ENRL.TAKEN_UNIT)
                          * NVL ((CLASSORG_FIN.PERCENT_OWNED / 100), 1))
                       / FP_ENRL.TOT_CREDITS
                   ELSE
                         (  (FP_ENRL.TAKEN_UNIT)
                          * NVL ((CLASSORG_FIN.PERCENT_OWNED / 100), 1))
                       / 12
               END,
               3)
               CLASS_FFTE,
           FP_ENRL.CLASS_NUM,
           FP_ENRL.CRSE_CD,
           FP_ENRL.SBJCT_CD,
           FP_ENRL.SBJCT_LD,
           FP_ENRL.CATALOG_NBR,
           FP_ENRL.CLASS_TITLE,
           FP_ENRL.CLASS_CAREER_CD,
           FP_ENRL.CLASS_CAREER_LD,
           FP_ENRL.INSTRCTN_MODE_CD,
           FP_ENRL.INSTRCTN_MODE_LD,
           FP_ENRL.CRSE_LVL,
           ROW_NUMBER ()
               OVER (
                   PARTITION BY CAST (FP_ENRL.INSTITUTION_CD AS VARCHAR2 (5)),
                                CAST (FP_ENRL.ACAD_CAR_CD AS VARCHAR2 (4)),
                                CAST (FP_ENRL.PERSON_ID AS VARCHAR2 (15)),
                                CAST (FP_ENRL.TERM_CD AS VARCHAR2 (4))
                   ORDER BY
                       CAST (FP_ENRL.INSTITUTION_CD AS VARCHAR2 (5)),
                       CAST (FP_ENRL.ACAD_CAR_CD AS VARCHAR2 (4)),
                       CAST (FP_ENRL.PERSON_ID AS VARCHAR2 (15)),
                       CAST (FP_ENRL.TERM_CD AS VARCHAR2 (4)),
                       FP_ENRL.CLASS_NUM,
                       CLASSORG_FIN.DEPTID)
               ROW_NUM,
           --FP_ENRL.ROW_NUM,
           FP_ENRL.CLASS_GRP_CD,
           FP_ENRL.CLASS_GRP_LD,
           FP_ENRL.CLASS_ORG_CD,
           FP_ENRL.CLASS_ORG_LD,
           CLASSORG_FIN.PERCENT_OWNED
               AS CLASS_PERCENT_OWNED,
           NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100
               AS CLASS_PERCENT_OWNED_CALC,
           CLASSORG_FIN.DEPTID
               AS CLASS_DEPTID,
           CLASSORG_FIN.DEPT_DESCR
               AS CLASS_DEPT_DESCR,
           CLASSORG_FIN.ORG_LEVEL_1
               AS CLASS_ORG_LEVEL_1,
           CLASSORG_FIN.ORG_LEVEL_1_DESCR
               AS CLASS_ORG_LEVEL_1_DESCR,
           CLASSORG_FIN.ORG_LEVEL_2
               AS CLASS_ORG_LEVEL_2,
           CLASSORG_FIN.ORG_LEVEL_2_DESCR
               AS CLASS_ORG_LEVEL_2_DESCR,
           CLASSORG_FIN.ORG_LEVEL_3
               AS CLASS_ORG_LEVEL_3,
           CLASSORG_FIN.ORG_LEVEL_3_DESCR
               AS CLASS_ORG_LEVEL_3_DESCR,
           CLASSORG_FIN.ORG_LEVEL_4
               AS CLASS_ORG_LEVEL_4,
           CLASSORG_FIN.ORG_LEVEL_4_DESCR
               AS CLASS_ORG_LEVEL_4_DESCR,
           CLASSORG_FIN.ORG_LEVEL_5
               AS CLASS_ORG_LEVEL_5,
           CLASSORG_FIN.ORG_LEVEL_5_DESCR
               AS CLASS_ORG_LEVEL_5_DESCR,
           CLASSORG_FIN.ORG_LEVEL_6
               AS CLASS_ORG_LEVEL_6,
           CLASSORG_FIN.ORG_LEVEL_6_DESCR
               AS CLASS_ORG_LEVEL_6_DESCR,
             SF_SUMM.GROSS_AMOUNT
           * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100)
               AS GROSS_AMOUNT,
           SF_SUMM.NET_AMOUNT * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100)
               AS NET_AMOUNT,
           SF_SUMM.AID_AMOUNT * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100)
               AS AID_AMOUNT,
             SF_SUMM.GRANT_AMOUNT
           * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100)
               AS GRANT_AMOUNT,
             SF_SUMM.SCHOLARSHIP_AMOUNT
           * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100)
               AS SCHOLARSHIP_AMOUNT,
             SF_SUMM.WAIVER_AMOUNT
           * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100)
               AS WAIVER_AMOUNT,
           ROUND (
                 (SF_SUMM.GROSS_AMOUNT / FP_ENRL.TOT_CREDITS)
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS GROSS_PER_CREDIT,
           ROUND (
                 (SF_SUMM.NET_AMOUNT / FP_ENRL.TOT_CREDITS)
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS NET_PER_CREDIT,
           ROUND (
                 (SF_SUMM.AID_AMOUNT / FP_ENRL.TOT_CREDITS)
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS AID_PER_CREDIT,
           ROUND (
                 (SF_SUMM.GRANT_AMOUNT / FP_ENRL.TOT_CREDITS)
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS GRANT_AID_PER_CREDIT,
           ROUND (
                 (SF_SUMM.SCHOLARSHIP_AMOUNT / FP_ENRL.TOT_CREDITS)
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS SCHOLARSHIP_AID_PER_CREDIT,
           ROUND (
                 (SF_SUMM.WAIVER_AMOUNT / FP_ENRL.TOT_CREDITS)
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS WAIVER_AID_PER_CREDIT,
           ROUND (
                 (SF_SUMM.FEDERAL_AID_AMOUNT / FP_ENRL.TOT_CREDITS)
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS FED_AID_PER_CREDIT,
           ROUND (
                 (SF_SUMM.STATE_AID_AMOUNT / FP_ENRL.TOT_CREDITS)
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS STATE_AID_PER_CREDIT,
           ROUND (
                 (SF_SUMM.INST_AID_AMOUNT / FP_ENRL.TOT_CREDITS)
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS INST_AID_PER_CREDIT,
           ROUND (
                 (SF_SUMM.PRIVATE_AID_AMOUNT / FP_ENRL.TOT_CREDITS)
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS PRIVATE_AID_PER_CREDIT,
           ROUND (
                 (SF_SUMM.GROSS_AMOUNT / FP_ENRL.TOT_CREDITS)
               * FP_ENRL.TAKEN_UNIT
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS GROSS_CONTRIB_TO_CRSE,
           ROUND (
                 (SF_SUMM.NET_AMOUNT / FP_ENRL.TOT_CREDITS)
               * FP_ENRL.TAKEN_UNIT
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS NET_CONTRIB_TO_CRSE,
           ROUND (
                 (SF_SUMM.AID_AMOUNT / FP_ENRL.TOT_CREDITS)
               * FP_ENRL.TAKEN_UNIT
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS AID_CONTRIB_TO_CRSE,
           ROUND (
                 (SF_SUMM.GRANT_AMOUNT / FP_ENRL.TOT_CREDITS)
               * FP_ENRL.TAKEN_UNIT
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS GRANT_AID_CONTRIB_TO_CRSE,
           ROUND (
                 (SF_SUMM.SCHOLARSHIP_AMOUNT / FP_ENRL.TOT_CREDITS)
               * FP_ENRL.TAKEN_UNIT
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS SCHOLARSHIP_AID_CONTRIB_TO_CRSE,
           ROUND (
                 (SF_SUMM.WAIVER_AMOUNT / FP_ENRL.TOT_CREDITS)
               * FP_ENRL.TAKEN_UNIT
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS WAIVER_AID_CONTRIB_TO_CRSE,
           ROUND (
                 (SF_SUMM.FEDERAL_AID_AMOUNT / FP_ENRL.TOT_CREDITS)
               * FP_ENRL.TAKEN_UNIT
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS FED_AID_CONTRIB_TO_CRSE,
           ROUND (
                 (SF_SUMM.STATE_AID_AMOUNT / FP_ENRL.TOT_CREDITS)
               * FP_ENRL.TAKEN_UNIT
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS STATE_AID_CONTRIB_TO_CRSE,
           ROUND (
                 (SF_SUMM.INST_AID_AMOUNT / FP_ENRL.TOT_CREDITS)
               * FP_ENRL.TAKEN_UNIT
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS INST_AID_CONTRIB_TO_CRSE,
           ROUND (
                 (SF_SUMM.PRIVATE_AID_AMOUNT / FP_ENRL.TOT_CREDITS)
               * FP_ENRL.TAKEN_UNIT
               * (NVL (CLASSORG_FIN.PERCENT_OWNED, 100) / 100),
               2)
               AS PRIVATE_AID_CONTRIB_TO_CRSE
      FROM DLMRT_OWNER.FIN_PLANNING_ENROLLMENT_VW  FP_ENRL
           LEFT JOIN DLMRT_OWNER.PS_D_ACAD_ORG_FIN_VW CLASSORG_FIN
               ON     FP_ENRL.INSTITUTION_CD = CLASSORG_FIN.INSTITUTION_CD
                  AND FP_ENRL.CLASS_ORG_CD = CLASSORG_FIN.ACAD_ORG_CD
           LEFT JOIN SF_SUMM
               ON     CAST (FP_ENRL.INSTITUTION_CD AS VARCHAR2 (5)) =
                      CAST (SF_SUMM.INSTITUTION_CD AS VARCHAR2 (5))
                  AND CAST (FP_ENRL.PERSON_ID AS VARCHAR2 (15)) =
                      CAST (SF_SUMM.PERSON_ID AS VARCHAR2 (15))
                  AND CAST (FP_ENRL.FISCAL_YEAR AS VARCHAR2 (4)) =
                      CAST (SF_SUMM.FISCAL_YEAR AS VARCHAR2 (4))
                  AND CAST (FP_ENRL.TERM_CD AS VARCHAR2 (4)) =
                      CAST (SF_SUMM.ITEM_TERM AS VARCHAR2 (4))
                  AND CAST (
                          DECODE (FP_ENRL.ACAD_CAR_CD,
                                  '-', 'XXXX',
                                  FP_ENRL.ACAD_CAR_CD)
                              AS VARCHAR2 (4)) =
                      CAST (
                          DECODE (SF_SUMM.ACAD_CAR_CD,
                                  '-', 'XXXX',
                                  SF_SUMM.ACAD_CAR_CD)
                              AS VARCHAR2 (4))
/
