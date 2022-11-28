DROP INDEX CSSTG_OWNER.PK_UM_TEST_RESULTS
/

--
-- PK_UM_TEST_RESULTS  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_UM_TEST_RESULTS ON CSSTG_OWNER.UM_TEST_RESULTS
(RUN_DT, TEST_SUBJECT, TEST_DESCR)
/
