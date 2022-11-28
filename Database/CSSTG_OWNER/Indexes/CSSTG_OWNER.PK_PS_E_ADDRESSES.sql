DROP INDEX CSSTG_OWNER.PK_PS_E_ADDRESSES
/

--
-- PK_PS_E_ADDRESSES  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_E_ADDRESSES ON CSSTG_OWNER.PS_E_ADDRESSES
(ERROR_SID, SRC_SYS_ID)
/
