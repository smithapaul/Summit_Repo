ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIGNATN MODIFY 
  SFA_ATB_STATE_CD NULL
/

ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIGNATN MODIFY 
  SFA_ATB_STATE_CD NOT NULL
  ENABLE VALIDATE
/
