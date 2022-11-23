DROP INDEX CSMRT_OWNER.NK_PS_D_PROG_ACN
/

--
-- NK_PS_D_PROG_ACN  (Index) 
--
CREATE UNIQUE INDEX CSMRT_OWNER.NK_PS_D_PROG_ACN ON CSMRT_OWNER.PS_D_PROG_ACN
(SETID, PROG_ACN_CD, SRC_SYS_ID)
/
