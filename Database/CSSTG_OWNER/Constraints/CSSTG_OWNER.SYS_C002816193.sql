ALTER TABLE CSSTG_OWNER.PS_AUDIT_APPROG_UM MODIFY 
  LOAD_ERROR NULL
/

ALTER TABLE CSSTG_OWNER.PS_AUDIT_APPROG_UM MODIFY 
  LOAD_ERROR NOT NULL
  ENABLE VALIDATE
/
