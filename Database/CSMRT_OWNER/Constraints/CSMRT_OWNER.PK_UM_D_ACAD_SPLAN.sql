ALTER TABLE CSMRT_OWNER.UM_D_ACAD_SPLAN
  DROP CONSTRAINT PK_UM_D_ACAD_SPLAN
/

ALTER TABLE CSMRT_OWNER.UM_D_ACAD_SPLAN ADD (
  CONSTRAINT PK_UM_D_ACAD_SPLAN
  PRIMARY KEY
  (ACAD_SPLAN_SID)
  RELY
  DISABLE NOVALIDATE)
/
