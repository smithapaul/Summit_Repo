ALTER TABLE CSMRT_OWNER.UM_F_SF_STDNT_EQUTN_VAR
  DROP CONSTRAINT PK_UM_F_SF_STDNT_EQUTN_VAR
/

ALTER TABLE CSMRT_OWNER.UM_F_SF_STDNT_EQUTN_VAR ADD (
  CONSTRAINT PK_UM_F_SF_STDNT_EQUTN_VAR
  PRIMARY KEY
  (INSTITUTION_CD, BILLING_CAREER, TERM_CD, PERSON_ID, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
