ALTER TABLE CSSTG_OWNER.PS_BI_BILLING_LINE MODIFY 
  BATCH_SID NULL
/

ALTER TABLE CSSTG_OWNER.PS_BI_BILLING_LINE MODIFY 
  BATCH_SID NOT NULL
  ENABLE VALIDATE
/
