ALTER TABLE CSMRT_OWNER.UM_D_CRSE
  DROP CONSTRAINT PK_UM_D_CRSE
/

ALTER TABLE CSMRT_OWNER.UM_D_CRSE ADD (
  CONSTRAINT PK_UM_D_CRSE
  PRIMARY KEY
  (CRSE_SID)
  RELY
  DISABLE NOVALIDATE)
/
