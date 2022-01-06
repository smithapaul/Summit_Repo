CREATE OR REPLACE VIEW UM_D_APPL_CNTR_VW
BEQUEATH DEFINER
AS 
SELECT APPL_CNTR_SID,
           INSTITUTION_CD,
           INSTITUTION_CD INSTITUTION_ID,   -- Temporary redundant column 
           APPL_CNTR_ID,
           SRC_SYS_ID,
           EFFDT,
           EFF_STAT_CD,
           APPL_CNTR_SD,
           APPL_CNTR_LD,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM PS_D_APPL_CNTR;
