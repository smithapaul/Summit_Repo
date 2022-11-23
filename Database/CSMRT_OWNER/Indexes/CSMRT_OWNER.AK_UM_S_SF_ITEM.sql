DROP INDEX CSMRT_OWNER.AK_UM_S_SF_ITEM
/

--
-- AK_UM_S_SF_ITEM  (Index) 
--
CREATE BITMAP INDEX CSMRT_OWNER.AK_UM_S_SF_ITEM ON CSMRT_OWNER.UM_S_SF_ITEM
(UM_FISCAL_YEAR)
  LOCAL (  
  PARTITION FY_2021
)
/
