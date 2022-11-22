DROP VIEW CSMRT_OWNER.UM_F_ADM_APPL_LST_SCHL_VW
/

--
-- UM_F_ADM_APPL_LST_SCHL_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_ADM_APPL_LST_SCHL_VW
BEQUEATH DEFINER
AS 
SELECT APPLCNT_SID,
           INSTITUTION_SID,
           EXT_ORG_SID,
           SRC_SYS_ID,
           INSTITUTION_CD,
           PERSON_ID,
           EXT_ORG_ID,
           EXT_DATA_NBR,                                    -- Added Sept 2015
           EXT_ACAD_CAR_ID,                                 -- Added Sept 2015
           EXT_SUMM_TYPE_ID,                                -- Added Sept 2015
           CLASS_RANK,
           CLASS_SIZE,
           CLASS_PERCENTILE,
           EXT_GPA,
           CONVERTED_GPA,
--           UM_CUM_CREDIT,
--           UM_CUM_GPA,
--           UM_CUM_QP,
case when INSTITUTION_CD = 'UMBOS' then UM_CUM_CREDIT else UM_CUM_CREDIT_AGG end UM_CUM_CREDIT,     -- Aug 2022
case when INSTITUTION_CD = 'UMBOS' then UM_CUM_GPA else UM_CUM_GPA_AGG end UM_CUM_GPA,              -- Aug 2022
case when INSTITUTION_CD = 'UMBOS' then UM_CUM_QP else UM_CUM_QP_AGG end UM_CUM_QP,                 -- Aug 2022
           UM_CUM_CREDIT_AGG,                               -- Aug 2022
           UM_CUM_GPA_AGG,                                  -- Aug 2022
           UM_CUM_QP_AGG,                                   -- Aug 2022
           UM_GPA_EXCLUDE_FLG,
           UM_EXT_ORG_CR,
           UM_EXT_ORG_QP,
           UM_EXT_ORG_GPA,
           UM_EXT_ORG_CNV_CR,
           UM_EXT_ORG_CNV_GPA,
           UM_EXT_ORG_CNV_QP,
           UM_GPA_OVRD_FLG,
           UM_1_OVRD_HSGPA_FLG,
           UM_CONVERT_GPA,
           UM_EXT_OR_MTSC_GPA,                              -- Added Sept 2019
           MS_CONVERT_GPA,                                  -- Added Sept 2019
           MAX_DATA_ROW,                                    -- Aug 2022
           DATA_ORIGIN,                                     -- Added Sept 2019
           CREATED_EW_DTTM,                                 -- Added Sept 2019
           LASTUPD_EW_DTTM                                  -- Added Sept 2019
      FROM CSMRT_OWNER.UM_F_ADM_APPL_LST_SCHL
/
