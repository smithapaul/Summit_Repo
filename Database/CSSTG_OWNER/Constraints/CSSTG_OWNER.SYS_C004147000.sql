ALTER TABLE CSSTG_OWNER.PS_SRVC_IN_RSN_TBL MODIFY 
  SRVC_IND_REASON NULL
/

ALTER TABLE CSSTG_OWNER.PS_SRVC_IN_RSN_TBL MODIFY 
  SRVC_IND_REASON NOT NULL
  ENABLE VALIDATE
/
