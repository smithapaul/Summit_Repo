ALTER TABLE CSSTG_OWNER.PS_UM_FA_GRAD_STG MODIFY 
  UM_AID_PKG_NEED NULL
/

ALTER TABLE CSSTG_OWNER.PS_UM_FA_GRAD_STG MODIFY 
  UM_AID_PKG_NEED NOT NULL
  ENABLE VALIDATE
/
