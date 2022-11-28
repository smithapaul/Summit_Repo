DROP INDEX CSSTG_OWNER.PK_PS_ADM_PRSPCT_PROG
/

--
-- PK_PS_ADM_PRSPCT_PROG  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_ADM_PRSPCT_PROG ON CSSTG_OWNER.PS_ADM_PRSPCT_PROG
(EMPLID, ACAD_CAREER, INSTITUTION, ACAD_PROG, SRC_SYS_ID)
/
