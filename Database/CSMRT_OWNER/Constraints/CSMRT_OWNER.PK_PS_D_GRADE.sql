ALTER TABLE CSMRT_OWNER.PS_D_GRADE
  DROP CONSTRAINT PK_PS_D_GRADE
/

ALTER TABLE CSMRT_OWNER.PS_D_GRADE ADD (
  CONSTRAINT PK_PS_D_GRADE
  PRIMARY KEY
  (GRADE_SID)
  RELY
  DISABLE NOVALIDATE)
/
