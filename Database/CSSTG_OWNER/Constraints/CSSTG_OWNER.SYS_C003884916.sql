ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIGNATN MODIFY 
  SFA_DL_ENDORS_APPR NULL
/

ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIGNATN MODIFY 
  SFA_DL_ENDORS_APPR NOT NULL
  ENABLE VALIDATE
/
