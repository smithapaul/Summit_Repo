ALTER TABLE CSMRT_OWNER.UM_F_EXT_TESTSCORE MODIFY 
  TEST_CMPNT_ID NULL
/

ALTER TABLE CSMRT_OWNER.UM_F_EXT_TESTSCORE MODIFY 
  TEST_CMPNT_ID NOT NULL
  ENABLE VALIDATE
/
