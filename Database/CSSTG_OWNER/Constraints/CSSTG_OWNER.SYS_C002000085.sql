ALTER TABLE CSSTG_OWNER.PS_LN_TYPE_TBL MODIFY 
  LN_MAX_NBR_DISBS NULL
/

ALTER TABLE CSSTG_OWNER.PS_LN_TYPE_TBL MODIFY 
  LN_MAX_NBR_DISBS NOT NULL
  ENABLE VALIDATE
/
