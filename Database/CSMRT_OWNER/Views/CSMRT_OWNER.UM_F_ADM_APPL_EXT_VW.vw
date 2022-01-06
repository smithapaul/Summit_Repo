CREATE OR REPLACE VIEW UM_F_ADM_APPL_EXT_VW
BEQUEATH DEFINER
AS 
SELECT APPLCNT_SID,
           INSTITUTION_SID,
           EXT_ORG_SID,
           EXT_ACAD_CAR_SID,
           EXT_DATA_NBR,
           EXT_SUMM_TYPE_SID,
           SRC_SYS_ID,
           PERSON_ID,
           INSTITUTION_CD,
           EXT_ORG_ID,
           EXT_ACAD_CAR_ID,
           EXT_SUMM_TYPE_ID,
           EXT_ACAD_LVL_SID,
           EXT_TERM_YEAR_SID,
           EXT_TERM_SID,
           ACAD_UNIT_TYPE_SID,
           ACAD_RANK_TYPE_SID,
           GPA_TYPE_SID,
           LS_DATA_SOURCE,
           LS_DATA_SOURCE_SD,
           LS_DATA_SOURCE_LD,
           TRNSCR_FLG,
           TRNSCR_FLG_LD,
           TRNSCR_TYPE,
           TRNSCR_TYPE_LD,
           TRNSCR_STATUS,
           TRNSCR_DT,
           --          (CASE
           --              WHEN FROM_DT IS NULL AND EXT_ORG_SID < 2147483646
           --              THEN
           --                 TO_DATE ('01-JAN-1900')
           --              ELSE
           --                 FROM_DT
           --           END)
           FROM_DT,
           --          (CASE
           --              WHEN TO_DT IS NULL AND EXT_ORG_SID < 2147483646
           --              THEN
           --                 TO_DATE ('01-JAN-1900')
           --              ELSE
           --                 TO_DT
           --           END)
           TO_DT,
           D_EXT_ACAD_LVL_SID,
           D_EXT_TERM_YEAR_SID,
           D_EXT_TERM_SID,
           UNITS_ATTMPTD,
           UNITS_CMPLTD,
           --          (CASE
           --              WHEN CLASS_RANK IS NULL AND EXT_ORG_SID < 2147483646 THEN 0
           --              ELSE CLASS_RANK
           --           END)
           CLASS_RANK,
           --          (CASE
           --              WHEN CLASS_SIZE IS NULL AND EXT_ORG_SID < 2147483646 THEN 0
           --              ELSE CLASS_SIZE
           --           END)
           CLASS_SIZE,
           --          (CASE
           --              WHEN CLASS_PERCENTILE IS NULL AND EXT_ORG_SID < 2147483646
           --              THEN
           --                 0
           --              ELSE
           --                 CLASS_PERCENTILE
           --           END)
           CLASS_PERCENTILE,
           --          (CASE
           --              WHEN EXT_GPA IS NULL AND EXT_ORG_SID < 2147483646 THEN 0
           --              ELSE EXT_GPA
           --           END)
           EXT_GPA,
           --          (CASE
           --              WHEN CONVERTED_GPA IS NULL AND EXT_ORG_SID < 2147483646 THEN 0
           --              ELSE CONVERTED_GPA
           --           END)
           CONVERTED_GPA,
           --          (CASE
           --              WHEN UM_CUM_CREDIT IS NULL AND EXT_ORG_SID < 2147483646 THEN 0
           --              ELSE UM_CUM_CREDIT
           --           END)
           UM_CUM_CREDIT,
           --          (CASE
           --              WHEN UM_CUM_GPA IS NULL AND EXT_ORG_SID < 2147483646 THEN 0
           --              ELSE UM_CUM_GPA
           --           END)
           UM_CUM_GPA,
           --          (CASE
           --              WHEN UM_CUM_QP IS NULL AND EXT_ORG_SID < 2147483646 THEN 0
           --              ELSE UM_CUM_QP
           --           END)
           UM_CUM_QP,
           UM_GPA_EXCLUDE_FLG,
           --          (CASE
           --              WHEN UM_EXT_ORG_CR IS NULL AND EXT_ORG_SID < 2147483646 THEN 0
           --              ELSE UM_EXT_ORG_CR
           --           END)
           UM_EXT_ORG_CR,
           --          (CASE
           --              WHEN UM_EXT_ORG_QP IS NULL AND EXT_ORG_SID < 2147483646 THEN 0
           --              ELSE UM_EXT_ORG_QP
           --           END)
           UM_EXT_ORG_QP,
           --          (CASE
           --              WHEN UM_EXT_ORG_GPA IS NULL AND EXT_ORG_SID < 2147483646 THEN 0
           --              ELSE UM_EXT_ORG_GPA
           --           END)
           UM_EXT_ORG_GPA,
           --          (CASE
           --              WHEN UM_EXT_ORG_CNV_CR IS NULL AND EXT_ORG_SID < 2147483646
           --              THEN
           --                 0
           --              ELSE
           --                 UM_EXT_ORG_CNV_CR
           --           END)
           UM_EXT_ORG_CNV_CR,
           --          (CASE
           --              WHEN UM_EXT_ORG_CNV_GPA IS NULL AND EXT_ORG_SID < 2147483646
           --              THEN
           --                 0
           --              ELSE
           --                 UM_EXT_ORG_CNV_GPA
           --           END)
           UM_EXT_ORG_CNV_GPA,
           --          (CASE
           --              WHEN UM_EXT_ORG_CNV_QP IS NULL AND EXT_ORG_SID < 2147483646
           --              THEN
           --                 0
           --              ELSE
           --                 UM_EXT_ORG_CNV_QP
           --           END)
           UM_EXT_ORG_CNV_QP,
           UM_GPA_OVRD_FLG,
           UM_1_OVRD_HSGPA_FLG,
           UM_OVRD_GPA_FLG,                             -- 8/20/2019, CSR 8335
           UM_CONVERT_GPA,
           ADM_APPL_EXT_ORDER,
           BEST_SUMM_TYPE_GPA_FLG,
           BEST_HS_GPA,
		   UM_EXT_OR_MTSC_GPA, 
		   MS_CONVERT_GPA,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM CSMRT_OWNER.UM_F_ADM_APPL_EXT
     WHERE ROWNUM < 1000000000;
