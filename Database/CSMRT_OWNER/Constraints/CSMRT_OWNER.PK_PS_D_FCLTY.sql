ALTER TABLE CSMRT_OWNER.PS_D_FCLTY
  DROP CONSTRAINT PK_PS_D_FCLTY
/

ALTER TABLE CSMRT_OWNER.PS_D_FCLTY ADD (
  CONSTRAINT PK_PS_D_FCLTY
  PRIMARY KEY
  (FCLTY_SID)
  RELY
  DISABLE NOVALIDATE)
/
