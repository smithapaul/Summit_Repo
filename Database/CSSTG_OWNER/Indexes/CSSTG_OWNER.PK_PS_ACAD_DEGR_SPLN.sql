DROP INDEX CSSTG_OWNER.PK_PS_ACAD_DEGR_SPLN
/

--
-- PK_PS_ACAD_DEGR_SPLN  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_ACAD_DEGR_SPLN ON CSSTG_OWNER.PS_ACAD_DEGR_SPLN
(EMPLID, STDNT_DEGR, ACAD_PLAN, ACAD_SUB_PLAN, SRC_SYS_ID)
/
