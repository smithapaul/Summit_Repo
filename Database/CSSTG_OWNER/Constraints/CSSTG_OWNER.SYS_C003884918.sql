ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIGNATN MODIFY 
  LOAD_ERROR NULL
/

ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIGNATN MODIFY 
  LOAD_ERROR NOT NULL
  ENABLE VALIDATE
/
