ALTER TABLE CSSTG_OWNER.PS_E_ADDRESSES MODIFY 
  TARGET_TABLE_TYPE NULL
/

ALTER TABLE CSSTG_OWNER.PS_E_ADDRESSES MODIFY 
  TARGET_TABLE_TYPE NOT NULL
  ENABLE VALIDATE
/
