DROP VIEW CSMRT_OWNER.UM_F_FA_STDNT_ISIR_STG_VW
/

--
-- UM_F_FA_STDNT_ISIR_STG_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_FA_STDNT_ISIR_STG_VW
BEQUEATH DEFINER
AS 
SELECT INSTITUTION_CD,
          PERSON_ID,
          AID_YEAR,
          ECQUEUEINSTANCE,
          ISIR_SEQ_NO,
          SRC_SYS_ID,
          INSTITUTION_SID,
          PERSON_SID,
          ISIR_LOAD_STATUS,
          ISIR_LOAD_STATUS_SD,
          ISIR_LOAD_STATUS_LD,
          ISIR_LOAD_ACTION,
          ISIR_LOAD_ACTION_SD,
          ISIR_LOAD_ACTION_LD,
          ADMIT_LVL,
          ADMIT_LVL_SD,
          ADMIT_LVL_LD,
          ORIG_SSN,
          SSN,
          IWD_STD_LAST_NAME,
          IWD_STD_FIRST_NM02,
          IWD_STU_MI,
          IWD_PERM_ADDR02,
          IWD_CITY,
          IWD_STATE,
          IWD_ZIP,
          BIRTHDATE,
          IWD_PERM_PHONE,
          TRANS_RECEIPT_DT,
          SUSPEND_REASON,
          SUSPEND_REASON_SD,
          SUSPEND_REASON_LD,
          IWD_TRANS_NBR,
          DEPNDNCY_STAT,
          DEPNDNCY_STAT_SD,
          DEPNDNCY_STAT_LD,
          TRANS_PROCESS_DT,
          IWD_PRIMARY_EFC,
          IWD_STD_EMAIL,
          IWD_SOURCE_CORR,
          IWD_EFC_CHNG_FLAG,
          ISIR_SAR_C_CHNG,
          LOAD_ERROR,
          DATA_ORIGIN,
          CREATED_EW_DTTM,
          LASTUPD_EW_DTTM,
          BATCH_SID
     FROM UM_F_FA_STDNT_ISIR_STG
    where ROWNUM < 1000000000
/
