ALTER TABLE CSMRT_OWNER.PS_D_PROG_ACN
  DROP CONSTRAINT PK_PS_D_PROG_ACN
/

ALTER TABLE CSMRT_OWNER.PS_D_PROG_ACN ADD (
  CONSTRAINT PK_PS_D_PROG_ACN
  PRIMARY KEY
  (PROG_ACN_SID, SETID)
  RELY
  DISABLE NOVALIDATE)
/
