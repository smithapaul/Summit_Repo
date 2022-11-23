ALTER TABLE CSMRT_OWNER.UM_F_TRANSFER
  DROP CONSTRAINT PK_UM_F_TRANSFER
/

ALTER TABLE CSMRT_OWNER.UM_F_TRANSFER ADD (
  CONSTRAINT PK_UM_F_TRANSFER
  PRIMARY KEY
  (INSTITUTION_CD, ACAD_CAR_CD, PERSON_ID, MODEL_NBR, ARTICULATION_TERM, TRNSFR_EQVLNCY_GRP, TRNSFR_EQVLNCY_SEQ, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
