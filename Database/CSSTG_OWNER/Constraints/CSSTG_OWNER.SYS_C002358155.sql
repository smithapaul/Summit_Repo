ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIG_MSG_OLD MODIFY 
  LOAN_TYPE NULL
/

ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIG_MSG_OLD MODIFY 
  LOAN_TYPE NOT NULL
  ENABLE VALIDATE
/
