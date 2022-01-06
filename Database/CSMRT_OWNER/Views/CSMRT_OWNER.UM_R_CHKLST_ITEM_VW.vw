CREATE OR REPLACE VIEW UM_R_CHKLST_ITEM_VW
BEQUEATH DEFINER
AS 
SELECT COMMON_ID, 
          SEQ_3C,
          CHECKLIST_SEQ,
          SRC_SYS_ID,
          INSTITUTION_CD, 
          INSTITUTION_SID, 
          PERSON_SID, 
          ITEM_CD_SID,
          ITEM_STATUS,
          ITEM_STATUS_SD,
          ITEM_STATUS_LD,
          STATUS_DT,
          STATUS_CHANGE_ID,
          DUE_DT,
          CURRENCY_CD,
          DUE_AMT,
          RESPONSIBLE_ID,
          ASSOC_ID,
          NAME,
          COMM_KEY,
--          ROW_NUMBER ()
--             OVER (PARTITION BY COMMON_ID, ITEM_CD_SID, SRC_SYS_ID
--                   ORDER BY
--                      COMMON_ID,
--                      ITEM_CD_SID,
--                      SRC_SYS_ID,
--                      SEQ_3C DESC,
--                      CHECKLIST_SEQ DESC)
             ITEM_ORDER,
          LOAD_ERROR,
          DATA_ORIGIN,
          CREATED_EW_DTTM,
          LASTUPD_EW_DTTM,
          BATCH_SID
     FROM PS_R_CHKLST_ITEM;
