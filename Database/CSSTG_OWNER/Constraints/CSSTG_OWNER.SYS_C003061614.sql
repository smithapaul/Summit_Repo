ALTER TABLE CSSTG_OWNER.PS_IMMUN_CRITERIA MODIFY 
  STATUS_IMMUN NULL
/

ALTER TABLE CSSTG_OWNER.PS_IMMUN_CRITERIA MODIFY 
  STATUS_IMMUN NOT NULL
  ENABLE VALIDATE
/
