ALTER TABLE CSMRT_OWNER.PS_D_CUR_TERM
  DROP CONSTRAINT PK_PS_D_CUR_TERM
/

ALTER TABLE CSMRT_OWNER.PS_D_CUR_TERM ADD (
  CONSTRAINT PK_PS_D_CUR_TERM
  PRIMARY KEY
  (INSTITUTION_CD, ACAD_CAR_CD, CURRENT_TERM, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
