ALTER TABLE CSSTG_OWNER.PS_S_COMPANY_TBL_OLD MODIFY 
  FED_RSRV_BANK_DIST NULL
/

ALTER TABLE CSSTG_OWNER.PS_S_COMPANY_TBL_OLD MODIFY 
  FED_RSRV_BANK_DIST NOT NULL
  ENABLE VALIDATE
/
