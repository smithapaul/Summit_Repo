ALTER TABLE CSMRT_OWNER.PS_D_VAR_DATA
  DROP CONSTRAINT PK_PS_D_VAR_DATA
/

ALTER TABLE CSMRT_OWNER.PS_D_VAR_DATA ADD (
  CONSTRAINT PK_PS_D_VAR_DATA
  PRIMARY KEY
  (VAR_DATA_SID)
  RELY
  DISABLE NOVALIDATE)
/
