CREATE OR REPLACE VIEW UM_D_RECRT_CNTR_VW
BEQUEATH DEFINER
AS 
SELECT RECRT_CNTR_SID,
           INSTITUTION_CD,
           INSTITUTION_CD INSTITUTION_ID,   -- Temporary redundant column 
           RECRT_CNTR_ID,
           SRC_SYS_ID,
           EFFDT,
           EFF_STAT_CD,
           RECRT_CNTR_SD,
           RECRT_CNTR_LD,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM PS_D_RECRT_CNTR;
