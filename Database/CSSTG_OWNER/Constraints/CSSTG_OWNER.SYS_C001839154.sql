ALTER TABLE CSSTG_OWNER.PS_BI_BILL_HEADER MODIFY 
  BILL_REQ_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_BI_BILL_HEADER MODIFY 
  BILL_REQ_ID NOT NULL
  ENABLE VALIDATE
/
