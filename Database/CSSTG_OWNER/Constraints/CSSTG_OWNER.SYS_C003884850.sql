ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIGNATN MODIFY 
  LN_TRNS_STU_SSN NULL
/

ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIGNATN MODIFY 
  LN_TRNS_STU_SSN NOT NULL
  ENABLE VALIDATE
/
