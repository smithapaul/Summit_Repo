ALTER TABLE CSSTG_OWNER.PS_S_RT_INDEX_TBL_OLD MODIFY 
  RT_RATE_INDEX NULL
/

ALTER TABLE CSSTG_OWNER.PS_S_RT_INDEX_TBL_OLD MODIFY 
  RT_RATE_INDEX NOT NULL
  ENABLE VALIDATE
/
