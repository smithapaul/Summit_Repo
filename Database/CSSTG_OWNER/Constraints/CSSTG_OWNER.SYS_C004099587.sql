ALTER TABLE CSSTG_OWNER.PS_ISIR_PARENT MODIFY 
  SFA_UNTAX_PENSION NULL
/

ALTER TABLE CSSTG_OWNER.PS_ISIR_PARENT MODIFY 
  SFA_UNTAX_PENSION NOT NULL
  ENABLE VALIDATE
/
