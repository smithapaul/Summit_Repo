ALTER TABLE CSSTG_OWNER.PS_S_ALTACCT_TBL_OLD MODIFY 
  STATISTICS_ACCOUNT NULL
/

ALTER TABLE CSSTG_OWNER.PS_S_ALTACCT_TBL_OLD MODIFY 
  STATISTICS_ACCOUNT NOT NULL
  ENABLE VALIDATE
/
