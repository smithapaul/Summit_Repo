ALTER TABLE CSSTG_OWNER.PS_UM_ADM_APP_TMP MODIFY 
  UM_PARENT2_EMPCOLL NULL
/

ALTER TABLE CSSTG_OWNER.PS_UM_ADM_APP_TMP MODIFY 
  UM_PARENT2_EMPCOLL NOT NULL
  ENABLE VALIDATE
/
