ALTER TABLE CSSTG_OWNER.PS_SRVC_IN_RSN_TBL MODIFY 
  SRVC_IN_REF_TYPE NULL
/

ALTER TABLE CSSTG_OWNER.PS_SRVC_IN_RSN_TBL MODIFY 
  SRVC_IN_REF_TYPE NOT NULL
  ENABLE VALIDATE
/
