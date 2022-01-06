CREATE OR REPLACE VIEW UM_D_RSDNCY_VW2
BEQUEATH DEFINER
AS 
SELECT RSDNCY_SID,
           RSDNCY_ID,
           SRC_SYS_ID,
           EFFDT,
           EFF_STAT_CD,
           RSDNCY_SD,
           RSDNCY_LD,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM PS_D_RSDNCY
     where ROWNUM < 10000;
