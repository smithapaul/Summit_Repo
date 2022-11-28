DROP INDEX CSSTG_OWNER.PK_PS_ACAD_ORG_FS_OWN_CW
/

--
-- PK_PS_ACAD_ORG_FS_OWN_CW  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_ACAD_ORG_FS_OWN_CW ON CSSTG_OWNER.PS_ACAD_ORG_FS_OWN_CW
(ACAD_ORG, BUSINESS_UNIT, DEPTID, EFFDT, SRC_SYS_ID)
/
