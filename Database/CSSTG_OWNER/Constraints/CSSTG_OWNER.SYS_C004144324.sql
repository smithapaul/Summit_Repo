ALTER TABLE CSSTG_OWNER.PSMSGCATDEFN MODIFY 
  LOAD_ERROR NULL
/

ALTER TABLE CSSTG_OWNER.PSMSGCATDEFN MODIFY 
  LOAD_ERROR NOT NULL
  ENABLE VALIDATE
/
