DROP VIEW CSMRT_OWNER.UM_D_FA_ITEM_TYPE_CTGRY_VW
/

--
-- UM_D_FA_ITEM_TYPE_CTGRY_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_FA_ITEM_TYPE_CTGRY_VW
BEQUEATH DEFINER
AS 
SELECT "INSTITUTION_CD",
           "AID_YEAR",
           "ITEM_TYPE",
           "INSTITUTION_SID",
           "ITEM_TYPE_SID",
           "REPORT_NAME",
           "CATEGORY_1",
           "CATEGORY_1_DESCR",
           "CATEGORY_2",
           "CATEGORY_2_DESCR",
           "CATEGORY_3",
           "CATEGORY_3_DESCR",
           "CATEGORY_4",
           "CATEGORY_4_DESCR",
           "CATEGORY_5",
           "CATEGORY_5_DESCR",
           "SRC_SYS_ID",
           "LOAD_ERROR",
           "DATA_ORIGIN",
           "CREATED_EW_DTTM",
           "LASTUPD_EW_DTTM",
           "BATCH_SID",
           "USE_FA_SF_BRIDGE",
           "AWARD_AMT_TYPE", 
           "NEED_BASED_CTGRY_FLG"
      FROM UM_D_FA_ITEM_TYPE_CTGRY
/
