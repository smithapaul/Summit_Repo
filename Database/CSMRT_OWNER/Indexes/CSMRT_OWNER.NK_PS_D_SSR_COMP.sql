DROP INDEX CSMRT_OWNER.NK_PS_D_SSR_COMP
/

--
-- NK_PS_D_SSR_COMP  (Index) 
--
CREATE UNIQUE INDEX CSMRT_OWNER.NK_PS_D_SSR_COMP ON CSMRT_OWNER.PS_D_SSR_COMP
(SSR_COMP_CD, SRC_SYS_ID)
/
