ALTER TABLE CSSTG_OWNER.PS_S_COMPANY_TBL_OLD MODIFY 
  FACT_CORRECT_BEL NULL
/

ALTER TABLE CSSTG_OWNER.PS_S_COMPANY_TBL_OLD MODIFY 
  FACT_CORRECT_BEL NOT NULL
  ENABLE VALIDATE
/
