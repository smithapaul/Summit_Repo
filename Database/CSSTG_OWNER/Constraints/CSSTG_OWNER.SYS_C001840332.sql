ALTER TABLE CSSTG_OWNER.PS_T_ACCOUNT_SF MODIFY 
  INCLUDE_BILLING NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_ACCOUNT_SF MODIFY 
  INCLUDE_BILLING NOT NULL
  ENABLE VALIDATE
/
