ALTER TABLE CSMRT_OWNER.UM_F_FA_PKG_VAR
  DROP CONSTRAINT PK_UM_F_FA_PKG_VAR
/

ALTER TABLE CSMRT_OWNER.UM_F_FA_PKG_VAR ADD (
  CONSTRAINT PK_UM_F_FA_PKG_VAR
  PRIMARY KEY
  (INSTITUTION_CD, ACAD_CAR_CD, AID_YEAR, PERSON_ID, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
