ALTER TABLE CSSTG_OWNER.PS_ADDRESSES MODIFY 
  IN_CITY_LIMIT NULL
/

ALTER TABLE CSSTG_OWNER.PS_ADDRESSES MODIFY 
  IN_CITY_LIMIT NOT NULL
  ENABLE VALIDATE
/
