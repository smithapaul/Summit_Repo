ALTER TABLE CSSTG_OWNER.PS_AGING_CAT_SF_OLD MODIFY 
  SRC_SYS_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_AGING_CAT_SF_OLD MODIFY 
  SRC_SYS_ID NOT NULL
  ENABLE VALIDATE
/
