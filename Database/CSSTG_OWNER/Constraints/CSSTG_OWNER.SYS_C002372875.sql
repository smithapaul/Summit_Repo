ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIG_ACTN_OLD MODIFY 
  DL_LN_APPL_ID_STAT NULL
/

ALTER TABLE CSSTG_OWNER.PS_LOAN_ORIG_ACTN_OLD MODIFY 
  DL_LN_APPL_ID_STAT NOT NULL
  ENABLE VALIDATE
/
