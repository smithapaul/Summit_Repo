ALTER TABLE CSSTG_OWNER.PS_T_LOCATION_TBL MODIFY 
  GEOLOC_CODE NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_LOCATION_TBL MODIFY 
  GEOLOC_CODE NOT NULL
  ENABLE VALIDATE
/
