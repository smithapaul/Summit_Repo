ALTER TABLE CSSTG_OWNER.PS_UM_ADM_APP_TMP MODIFY 
  UM_ADM_BA_MASTER NULL
/

ALTER TABLE CSSTG_OWNER.PS_UM_ADM_APP_TMP MODIFY 
  UM_ADM_BA_MASTER NOT NULL
  ENABLE VALIDATE
/
