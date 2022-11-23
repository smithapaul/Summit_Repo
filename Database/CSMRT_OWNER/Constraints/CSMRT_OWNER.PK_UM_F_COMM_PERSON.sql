ALTER TABLE CSMRT_OWNER.UM_F_COMM_PERSON
  DROP CONSTRAINT PK_UM_F_COMM_PERSON
/

ALTER TABLE CSMRT_OWNER.UM_F_COMM_PERSON ADD (
  CONSTRAINT PK_UM_F_COMM_PERSON
  PRIMARY KEY
  (COMMON_ID, SEQ_3C, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
