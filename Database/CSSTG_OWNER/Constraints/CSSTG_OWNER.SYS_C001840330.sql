ALTER TABLE CSSTG_OWNER.PS_T_ACCOUNT_SF MODIFY 
  OVERR_BILL_REQ_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_ACCOUNT_SF MODIFY 
  OVERR_BILL_REQ_ID NOT NULL
  ENABLE VALIDATE
/
