ALTER TABLE CSMRT_OWNER.PS_D_SESSION
  DROP CONSTRAINT PK_PS_D_SESSION
/

ALTER TABLE CSMRT_OWNER.PS_D_SESSION ADD (
  CONSTRAINT PK_PS_D_SESSION
  PRIMARY KEY
  (SESSION_SID)
  RELY
  DISABLE NOVALIDATE)
/
