ALTER TABLE CSSTG_OWNER.PS_EXTERNAL_SYSTEM MODIFY 
  EXTERNAL_SYSTEM_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_EXTERNAL_SYSTEM MODIFY 
  EXTERNAL_SYSTEM_ID NOT NULL
  ENABLE VALIDATE
/
