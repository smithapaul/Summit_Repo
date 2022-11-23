ALTER TABLE CSMRT_OWNER.UM_R_FA_STDNT_SR
  DROP CONSTRAINT PK_UM_R_FA_STDNT_SR
/

ALTER TABLE CSMRT_OWNER.UM_R_FA_STDNT_SR ADD (
  CONSTRAINT PK_UM_R_FA_STDNT_SR
  PRIMARY KEY
  (INSTITUTION_CD, PERSON_ID, SRC_SYS_ID, TERM_SID)
  RELY
  ENABLE VALIDATE)
/
