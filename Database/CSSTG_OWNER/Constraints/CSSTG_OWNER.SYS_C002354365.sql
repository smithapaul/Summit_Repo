ALTER TABLE CSSTG_OWNER.PS_SF_ACCTG_LN MODIFY 
  AUDIT_ACTN NULL
/

ALTER TABLE CSSTG_OWNER.PS_SF_ACCTG_LN MODIFY 
  AUDIT_ACTN NOT NULL
  ENABLE VALIDATE
/
