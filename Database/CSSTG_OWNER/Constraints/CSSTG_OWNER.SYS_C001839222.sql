ALTER TABLE CSSTG_OWNER.PS_T_BI_BILL_HEADER MODIFY 
  BI_REQ_NBR NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_BI_BILL_HEADER MODIFY 
  BI_REQ_NBR NOT NULL
  ENABLE VALIDATE
/
