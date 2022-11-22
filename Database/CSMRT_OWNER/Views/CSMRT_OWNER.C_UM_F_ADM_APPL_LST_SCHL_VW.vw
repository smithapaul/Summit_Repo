DROP VIEW CSMRT_OWNER.C_UM_F_ADM_APPL_LST_SCHL_VW
/

--
-- C_UM_F_ADM_APPL_LST_SCHL_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_UM_F_ADM_APPL_LST_SCHL_VW
BEQUEATH DEFINER
AS 
SELECT APPLCNT_SID,
           INSTITUTION_SID,
           EXT_ORG_SID,
           SRC_SYS_ID,
           PERSON_ID,
           INSTITUTION_CD,
           EXT_ORG_ID,
           EXT_DATA_NBR,
           EXT_ACAD_CAR_ID,
           EXT_SUMM_TYPE_ID,
           CLASS_RANK,
           CLASS_SIZE,
           CLASS_PERCENTILE,
           EXT_GPA,
           CONVERTED_GPA,
           UM_CUM_CREDIT,
           UM_CUM_GPA,
           UM_CUM_QP,
           UM_GPA_EXCLUDE_FLG,
           UM_EXT_ORG_CR,
           UM_EXT_ORG_QP,
           UM_EXT_ORG_GPA,
           UM_EXT_ORG_CNV_CR,
           UM_EXT_ORG_CNV_GPA,
           UM_EXT_ORG_CNV_QP,
           UM_GPA_OVRD_FLG,
           UM_1_OVRD_HSGPA_FLG,
           UM_CONVERT_GPA
      FROM CSMRT_OWNER.UM_F_ADM_APPL_LST_SCHL
/
