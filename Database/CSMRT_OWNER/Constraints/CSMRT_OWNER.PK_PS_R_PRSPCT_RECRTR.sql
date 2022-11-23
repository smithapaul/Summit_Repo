ALTER TABLE CSMRT_OWNER.PS_R_PRSPCT_RECRTR
  DROP CONSTRAINT PK_PS_R_PRSPCT_RECRTR
/

ALTER TABLE CSMRT_OWNER.PS_R_PRSPCT_RECRTR ADD (
  CONSTRAINT PK_PS_R_PRSPCT_RECRTR
  PRIMARY KEY
  (INSTITUTION_CD, ACAD_CAR_CD, PERSON_ID, RECRTR_ID, RECRT_CTGRY_ID, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
