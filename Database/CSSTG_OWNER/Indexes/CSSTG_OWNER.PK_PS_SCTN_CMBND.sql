DROP INDEX CSSTG_OWNER.PK_PS_SCTN_CMBND
/

--
-- PK_PS_SCTN_CMBND  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_SCTN_CMBND ON CSSTG_OWNER.PS_SCTN_CMBND
(INSTITUTION, STRM, SESSION_CODE, CLASS_NBR, SRC_SYS_ID)
/
