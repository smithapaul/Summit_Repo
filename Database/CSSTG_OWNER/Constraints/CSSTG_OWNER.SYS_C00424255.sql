ALTER TABLE CSSTG_OWNER.PS_S_ALTACCT_TBL_OLD MODIFY 
  BALANCE_FWD_SW NULL
/

ALTER TABLE CSSTG_OWNER.PS_S_ALTACCT_TBL_OLD MODIFY 
  BALANCE_FWD_SW NOT NULL
  ENABLE VALIDATE
/
