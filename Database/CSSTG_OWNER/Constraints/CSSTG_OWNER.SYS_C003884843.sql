ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIGNATN MODIFY 
  LN_TRNS_BORR_DFLT NULL
/

ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIGNATN MODIFY 
  LN_TRNS_BORR_DFLT NOT NULL
  ENABLE VALIDATE
/
