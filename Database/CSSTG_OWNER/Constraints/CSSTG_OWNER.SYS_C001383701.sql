ALTER TABLE CSSTG_OWNER.PS_UM_FA_GRAD_STG MODIFY 
  UM_LOAN_DECLINE NULL
/

ALTER TABLE CSSTG_OWNER.PS_UM_FA_GRAD_STG MODIFY 
  UM_LOAN_DECLINE NOT NULL
  ENABLE VALIDATE
/
