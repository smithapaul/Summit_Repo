ALTER TABLE CSSTG_OWNER.PS_ISIR_CONTROL MODIFY 
  FATHER_SSN_MATCH NULL
/

ALTER TABLE CSSTG_OWNER.PS_ISIR_CONTROL MODIFY 
  FATHER_SSN_MATCH NOT NULL
  ENABLE VALIDATE
/
