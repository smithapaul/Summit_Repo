ALTER TABLE CSMRT_OWNER.UM_F_FA_ISIR_AUDIT MODIFY 
  ISIR_TXN_NBR NULL
/

ALTER TABLE CSMRT_OWNER.UM_F_FA_ISIR_AUDIT MODIFY 
  ISIR_TXN_NBR NOT NULL
  ENABLE VALIDATE
/
