ALTER TABLE CSMRT_OWNER.UM_D_TRANSFER_DICT MODIFY 
  TRNSFR_CRSE_FLG NULL
/

ALTER TABLE CSMRT_OWNER.UM_D_TRANSFER_DICT MODIFY 
  TRNSFR_CRSE_FLG NOT NULL
  ENABLE VALIDATE
/
