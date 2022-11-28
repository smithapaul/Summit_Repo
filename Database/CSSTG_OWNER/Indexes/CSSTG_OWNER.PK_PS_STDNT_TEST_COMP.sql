DROP INDEX CSSTG_OWNER.PK_PS_STDNT_TEST_COMP
/

--
-- PK_PS_STDNT_TEST_COMP  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_STDNT_TEST_COMP ON CSSTG_OWNER.PS_STDNT_TEST_COMP
(EMPLID, TEST_ID, TEST_COMPONENT, TEST_DT, LS_DATA_SOURCE, 
SRC_SYS_ID)
/
