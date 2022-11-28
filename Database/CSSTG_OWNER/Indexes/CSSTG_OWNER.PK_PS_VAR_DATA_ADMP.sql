DROP INDEX CSSTG_OWNER.PK_PS_VAR_DATA_ADMP
/

--
-- PK_PS_VAR_DATA_ADMP  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_VAR_DATA_ADMP ON CSSTG_OWNER.PS_VAR_DATA_ADMP
(COMMON_ID, VAR_DATA_SEQ, SRC_SYS_ID)
/
