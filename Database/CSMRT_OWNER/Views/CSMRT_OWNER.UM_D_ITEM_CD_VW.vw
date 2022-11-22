DROP VIEW CSMRT_OWNER.UM_D_ITEM_CD_VW
/

--
-- UM_D_ITEM_CD_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_ITEM_CD_VW
BEQUEATH DEFINER
AS 
SELECT /*+ OPT_ESTIMATE(TABLE PS_D_ITEM_CD MIN=10000) */
          ITEM_CD_SID,
          CHKLST_ITEM_CD,
          SRC_SYS_ID,
          EFFDT,
          EFF_STAT_CD,
--          case when CHKLST_ITEM_CD like 'B%'
--               then 'UMBOS'
--               when CHKLST_ITEM_CD like 'F%'
--               then 'UMBOS'
--               else '-'
--           end 
               UMBOS_INSTITUTION_CD,            -- Added Feb 2017 
--          case when CHKLST_ITEM_CD like 'D%'
--               then 'UMDAR'
--               when CHKLST_ITEM_CD like 'F%'
--               then 'UMDAR'
--               else '-'
--           end 
               UMDAR_INSTITUTION_CD,            -- Added Feb 2017
--          case when CHKLST_ITEM_CD like 'L%'
--               then 'UMLOW'
--               when CHKLST_ITEM_CD like 'F%'
--               then 'UMLOW'
--               else '-'
--           end 
               UMLOW_INSTITUTION_CD,            -- Added Feb 2017
          ITEM_ASSOCIATION,
          ITEM_CD_SD,
          ITEM_CD_LD,
--          EFF_START_DT,
--          EFF_END_DT,
--          CURRENT_IND,
          LOAD_ERROR,
          DATA_ORIGIN,
          CREATED_EW_DTTM,
          LASTUPD_EW_DTTM,
          BATCH_SID
     FROM CSMRT_OWNER.PS_D_ITEM_CD
    WHERE ROWNUM < 100000
/
