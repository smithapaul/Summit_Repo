ALTER TABLE CSSTG_OWNER.PS_UM_ADM_APP_TMP MODIFY 
  UM_ADM_DG_PORT_SR NULL
/

ALTER TABLE CSSTG_OWNER.PS_UM_ADM_APP_TMP MODIFY 
  UM_ADM_DG_PORT_SR NOT NULL
  ENABLE VALIDATE
/
