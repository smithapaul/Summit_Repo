ALTER TABLE CSSTG_OWNER.PS_SRVC_IND_DATA MODIFY 
  SRVC_IND_REFRNCE NULL
/

ALTER TABLE CSSTG_OWNER.PS_SRVC_IND_DATA MODIFY 
  SRVC_IND_REFRNCE NOT NULL
  ENABLE VALIDATE
/
