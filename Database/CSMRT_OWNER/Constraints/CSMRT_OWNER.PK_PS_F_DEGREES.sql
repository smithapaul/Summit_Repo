ALTER TABLE CSMRT_OWNER.PS_F_DEGREES
  DROP CONSTRAINT PK_PS_F_DEGREES
/

ALTER TABLE CSMRT_OWNER.PS_F_DEGREES ADD (
  CONSTRAINT PK_PS_F_DEGREES
  PRIMARY KEY
  (PERSON_ID, DEGREE_NBR, ACAD_PLAN_CD, ACAD_SPLAN_CD, SRC_SYS_ID)
  RELY
  ENABLE VALIDATE)
/
