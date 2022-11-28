DROP INDEX CSSTG_OWNER.PK_PS_REG_REGION_TBL
/

--
-- PK_PS_REG_REGION_TBL  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_REG_REGION_TBL ON CSSTG_OWNER.PS_REG_REGION_TBL
(REG_REGION, SRC_SYS_ID)
/
