CREATE OR REPLACE VIEW C_UM_D_REPEAT_VW
BEQUEATH DEFINER
AS 
SELECT CAST (REPEAT_SID AS NUMBER (10))         REPEAT_SID,
           SETID,
           REPEAT_SCHEME_CD,
           REPEAT_SCHEME_SD,
           REPEAT_SCHEME_LD,
           REPEAT_CD,
           EFFDT,
           REPEAT_SD,
           REPEAT_LD,
           REPEAT_FD,
           SETID                                    SRC_SETID,
           TO_DATE ('1/1/1753', 'MM/DD/YYYY')       EFF_START_DT,
           TO_DATE ('12/31/9999', 'MM/DD/YYYY')     EFF_END_DT,
           CAST ('NULL' AS VARCHAR2 (1))            CURRENT_IND,
           SRC_SYS_ID,
           CAST ('Y' AS VARCHAR2 (1))               LOAD_ERROR,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM,
           CAST (1234 AS NUMBER (10))               BATCH_SID
      FROM csmrt_owner.PS_D_REPEAT;
