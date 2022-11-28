DROP INDEX CSSTG_OWNER.PK_PS_CRSE_FEE_TBL
/

--
-- PK_PS_CRSE_FEE_TBL  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_CRSE_FEE_TBL ON CSSTG_OWNER.PS_CRSE_FEE_TBL
(SETID, CRSE_ID, SSR_COMPONENT, INSTITUTION, CAMPUS, 
LOCATION, STRM, SESSION_CODE, SRC_SYS_ID)
/
