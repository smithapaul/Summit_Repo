DROP VIEW CSMRT_OWNER.UM_F_FA_STDNT_BDGT_ITEM_VW
/

--
-- UM_F_FA_STDNT_BDGT_ITEM_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_F_FA_STDNT_BDGT_ITEM_VW
BEQUEATH DEFINER
AS 
SELECT 
           INSTITUTION_CD,
           ACAD_CAR_CD,
           AID_YEAR,
           TERM_CD,
           PERSON_ID,
           BGT_ITEM_CATEGORY,
           SRC_SYS_ID,
           EFFDT,
           EFFSEQ,
           INSTITUTION_SID,
           ACAD_CAR_SID,
           TERM_SID,
           PERSON_SID,
           BGT_ITEM_CATEGORY_LD,
           BUDGET_ITEM_CD,
           BUDGET_ITEM_CD_LD,
           BUDGET_ITEM_AMOUNT,
           OPRID,
           PELL_ITEM_AMOUNT,
           SFA_PELITMAMT_LHT,
           LOAD_ERROR, 
           DATA_ORIGIN, 
           CREATED_EW_DTTM, 
           LASTUPD_EW_DTTM, 
           BATCH_SID
      FROM UM_F_FA_STDNT_BDGT_ITEM
/
