ALTER TABLE CSMRT_OWNER.UM_F_SF_ITEM_LINE MODIFY 
  ACCOUNT_TERM_SID NULL
/

ALTER TABLE CSMRT_OWNER.UM_F_SF_ITEM_LINE MODIFY 
  ACCOUNT_TERM_SID NOT NULL
  ENABLE VALIDATE
/
