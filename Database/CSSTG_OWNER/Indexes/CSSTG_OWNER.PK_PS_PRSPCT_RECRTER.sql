DROP INDEX CSSTG_OWNER.PK_PS_PRSPCT_RECRTER
/

--
-- PK_PS_PRSPCT_RECRTER  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_PRSPCT_RECRTER ON CSSTG_OWNER.PS_PRSPCT_RECRTER
(EMPLID, ACAD_CAREER, INSTITUTION, RECRUITMENT_CAT, RECRUITER_ID, 
SRC_SYS_ID)
/
