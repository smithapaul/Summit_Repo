ALTER TABLE CSSTG_OWNER.PS_BI_BILL_HEADER MODIFY 
  ADDR_FIELD2 NULL
/

ALTER TABLE CSSTG_OWNER.PS_BI_BILL_HEADER MODIFY 
  ADDR_FIELD2 NOT NULL
  ENABLE VALIDATE
/
