ALTER TABLE CSSTG_OWNER.PS_ADM_APPLCTR_TBL MODIFY 
  SF_MERCHANT_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_ADM_APPLCTR_TBL MODIFY 
  SF_MERCHANT_ID NOT NULL
  ENABLE VALIDATE
/
