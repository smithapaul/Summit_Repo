ALTER TABLE CSSTG_OWNER.PS_T_LOCATION_TBL MODIFY 
  COUNTRY_CODE NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_LOCATION_TBL MODIFY 
  COUNTRY_CODE NOT NULL
  ENABLE VALIDATE
/
