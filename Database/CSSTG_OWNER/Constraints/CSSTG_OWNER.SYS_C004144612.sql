ALTER TABLE CSSTG_OWNER.PS_T_CRSE_CATALOG MODIFY 
  FEES_EXIST NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_CRSE_CATALOG MODIFY 
  FEES_EXIST NOT NULL
  ENABLE VALIDATE
/
