ALTER TABLE CSSTG_OWNER.PS_S_CURR_CD_TBL_OLD MODIFY 
  CURRENCY_CD NULL
/

ALTER TABLE CSSTG_OWNER.PS_S_CURR_CD_TBL_OLD MODIFY 
  CURRENCY_CD NOT NULL
  ENABLE VALIDATE
/
