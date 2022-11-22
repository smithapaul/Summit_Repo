DROP VIEW CSMRT_OWNER.UM_F_CHKLST_PERSON_VW
/

--
-- UM_F_CHKLST_PERSON_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_CHKLST_PERSON_VW
BEQUEATH DEFINER
AS 
SELECT COMMON_ID,
           SEQ_3C,
           SRC_SYS_ID,
           INSTITUTION_CD,
           ADMIN_FUNC_SID,
           CHKLIST_CD_SID,
           CHKLIST_STAT_SID,
           DEPT_SID,
           INSTITUTION_SID,
           PERSON_SID,
           RESPONSIBLE_SID,
           STAT_CHG_SID,
           VAR_DATA_SID,
           CHKLIST_DT,
           CHKLIST_TM,
           DUE_DT,
           STATUS_DT,
           TRACKING_SEQ,
           CURRENCY_CD,
           DUE_AMT,
           ADMIN_FUNC_AREA,
           CHKLIST_ORDER,
--           LOAD_ERROR,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM,
--           BATCH_SID
           COMM_COMMENTS
      FROM PS_F_CHKLST_PERSON
/
