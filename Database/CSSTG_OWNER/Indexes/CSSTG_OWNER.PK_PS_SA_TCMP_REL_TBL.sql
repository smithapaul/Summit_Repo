DROP INDEX CSSTG_OWNER.PK_PS_SA_TCMP_REL_TBL
/

--
-- PK_PS_SA_TCMP_REL_TBL  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_SA_TCMP_REL_TBL ON CSSTG_OWNER.PS_SA_TCMP_REL_TBL
(TEST_ID, EFFDT, TEST_COMPONENT, SRC_SYS_ID)
/
