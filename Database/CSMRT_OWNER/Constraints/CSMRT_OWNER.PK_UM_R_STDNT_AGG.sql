ALTER TABLE CSMRT_OWNER.UM_R_STDNT_AGG
  DROP CONSTRAINT PK_UM_R_STDNT_AGG
/

ALTER TABLE CSMRT_OWNER.UM_R_STDNT_AGG ADD (
  CONSTRAINT PK_UM_R_STDNT_AGG
  PRIMARY KEY
  (TERM_SID, PERSON_SID, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
