ALTER TABLE CSMRT_OWNER.UM_D_CRSE_COMP
  DROP CONSTRAINT PK_UM_D_CRSE_COMP
/

ALTER TABLE CSMRT_OWNER.UM_D_CRSE_COMP ADD (
  CONSTRAINT PK_UM_D_CRSE_COMP
  PRIMARY KEY
  (CRSE_COMP_SID)
  RELY
  DISABLE NOVALIDATE)
/
