ALTER TABLE CSSTG_OWNER.PS_VISA_PERMIT_TBL MODIFY 
  VISA_PERMIT_TYPE NULL
/

ALTER TABLE CSSTG_OWNER.PS_VISA_PERMIT_TBL MODIFY 
  VISA_PERMIT_TYPE NOT NULL
  ENABLE VALIDATE
/
