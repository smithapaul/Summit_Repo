DROP INDEX CSMRT_OWNER.NK_PS_D_RECRT_STAT
/

--
-- NK_PS_D_RECRT_STAT  (Index) 
--
CREATE UNIQUE INDEX CSMRT_OWNER.NK_PS_D_RECRT_STAT ON CSMRT_OWNER.PS_D_RECRT_STAT
(RECRT_STAT_ID, SRC_SYS_ID)
/
