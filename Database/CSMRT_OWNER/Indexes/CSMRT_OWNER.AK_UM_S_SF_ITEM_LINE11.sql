DROP INDEX CSMRT_OWNER.AK_UM_S_SF_ITEM_LINE11
/

--
-- AK_UM_S_SF_ITEM_LINE11  (Index) 
--
CREATE BITMAP INDEX CSMRT_OWNER.AK_UM_S_SF_ITEM_LINE11 ON CSMRT_OWNER.UM_S_SF_ITEM_LINE
(ACCOUNT_TERM_SID)
  LOCAL (  
  PARTITION FY_2021
)
/
