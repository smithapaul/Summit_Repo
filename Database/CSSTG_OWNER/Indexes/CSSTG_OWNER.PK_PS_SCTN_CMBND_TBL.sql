DROP INDEX CSSTG_OWNER.PK_PS_SCTN_CMBND_TBL
/

--
-- PK_PS_SCTN_CMBND_TBL  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_SCTN_CMBND_TBL ON CSSTG_OWNER.PS_SCTN_CMBND_TBL
(INSTITUTION, STRM, SESSION_CODE, SCTN_COMBINED_ID, SRC_SYS_ID)
/
