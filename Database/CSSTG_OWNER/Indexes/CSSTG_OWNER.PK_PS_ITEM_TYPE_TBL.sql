DROP INDEX CSSTG_OWNER.PK_PS_ITEM_TYPE_TBL
/

--
-- PK_PS_ITEM_TYPE_TBL  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_ITEM_TYPE_TBL ON CSSTG_OWNER.PS_ITEM_TYPE_TBL
(SETID, ITEM_TYPE, EFFDT, SRC_SYS_ID)
/
