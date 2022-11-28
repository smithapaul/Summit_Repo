DROP INDEX CSSTG_OWNER.PK_PS_UM_FA_PRES_ITEM
/

--
-- PK_PS_UM_FA_PRES_ITEM  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_UM_FA_PRES_ITEM ON CSSTG_OWNER.PS_UM_FA_PRES_ITEM
(SETID, UM_FA_PRES_MODEL, AID_YEAR, UM_FA_PRES_PROG, ITEM_TYPE, 
SRC_SYS_ID)
/
