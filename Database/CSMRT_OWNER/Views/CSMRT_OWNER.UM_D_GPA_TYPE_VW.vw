DROP VIEW CSMRT_OWNER.UM_D_GPA_TYPE_VW
/

--
-- UM_D_GPA_TYPE_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_GPA_TYPE_VW
BEQUEATH DEFINER
AS 
SELECT GPA_TYPE_SID,
           INSTITUTION_CD,
           INSTITUTION_CD INSTITUTION_ID,   -- Temporary redundant column 
           GPA_TYPE_ID,
           SRC_SYS_ID,
           EFFDT,
           EFF_STAT_CD,
           GPA_TYPE_SD,
           GPA_TYPE_LD,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM PS_D_GPA_TYPE
/
