ALTER TABLE CSSTG_OWNER.PS_S_COMPANY_TBL_OLD MODIFY 
  PAYSHEET_LINES NULL
/

ALTER TABLE CSSTG_OWNER.PS_S_COMPANY_TBL_OLD MODIFY 
  PAYSHEET_LINES NOT NULL
  ENABLE VALIDATE
/
