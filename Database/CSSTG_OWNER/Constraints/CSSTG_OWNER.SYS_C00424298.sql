ALTER TABLE CSSTG_OWNER.PS_S_COMPANY_TBL_OLD MODIFY 
  IN_CITY_LIMIT NULL
/

ALTER TABLE CSSTG_OWNER.PS_S_COMPANY_TBL_OLD MODIFY 
  IN_CITY_LIMIT NOT NULL
  ENABLE VALIDATE
/
