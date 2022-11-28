DROP INDEX CSSTG_OWNER.PK_PS_STATE_TBL
/

--
-- PK_PS_STATE_TBL  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_STATE_TBL ON CSSTG_OWNER.PS_STATE_TBL
(COUNTRY, STATE, SRC_SYS_ID)
/
