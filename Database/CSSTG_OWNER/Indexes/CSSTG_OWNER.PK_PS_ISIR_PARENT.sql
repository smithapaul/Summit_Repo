DROP INDEX CSSTG_OWNER.PK_PS_ISIR_PARENT
/

--
-- PK_PS_ISIR_PARENT  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_ISIR_PARENT ON CSSTG_OWNER.PS_ISIR_PARENT
(EMPLID, INSTITUTION, AID_YEAR, EFFDT, EFFSEQ, 
TABLE_ID, SRC_SYS_ID)
/
