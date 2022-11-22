DROP VIEW CSMRT_OWNER.UM_F_STDNT_VW
/

--
-- UM_F_STDNT_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_STDNT_VW
BEQUEATH DEFINER
AS 
SELECT INSTITUTION_CD,
           ACAD_CAR_CD,
           PERSON_ID,
           SRC_SYS_ID,
           INSTITUTION_SID,
           ACAD_CAR_SID,
           PERSON_SID,
           ADM_CNT,
           SR_CNT,
           PRSPCT_CNT,
           LOAD_ERROR,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM,
           BATCH_SID
      FROM UM_F_STDNT
/
