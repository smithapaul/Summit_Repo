ALTER TABLE CSMRT_OWNER.UM_F_ADM_APPL_TRANSFER MODIFY 
  TRNSFR_SRC_TYPE_LD NULL
/

ALTER TABLE CSMRT_OWNER.UM_F_ADM_APPL_TRANSFER MODIFY 
  TRNSFR_SRC_TYPE_LD NOT NULL
  ENABLE VALIDATE
/
