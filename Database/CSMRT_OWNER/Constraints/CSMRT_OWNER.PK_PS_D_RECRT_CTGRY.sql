ALTER TABLE CSMRT_OWNER.PS_D_RECRT_CTGRY
  DROP CONSTRAINT PK_PS_D_RECRT_CTGRY
/

ALTER TABLE CSMRT_OWNER.PS_D_RECRT_CTGRY ADD (
  CONSTRAINT PK_PS_D_RECRT_CTGRY
  PRIMARY KEY
  (RECRT_CTGRY_SID)
  RELY
  DISABLE NOVALIDATE)
/
