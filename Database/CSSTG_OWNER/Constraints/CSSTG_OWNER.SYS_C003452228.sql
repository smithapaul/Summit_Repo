ALTER TABLE CSSTG_OWNER.PS_ISIR_COMPUTED MODIFY 
  FICA_TAX_PD NULL
/

ALTER TABLE CSSTG_OWNER.PS_ISIR_COMPUTED MODIFY 
  FICA_TAX_PD NOT NULL
  ENABLE VALIDATE
/
