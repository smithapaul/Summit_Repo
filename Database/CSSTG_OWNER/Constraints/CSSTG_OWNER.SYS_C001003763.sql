ALTER TABLE CSSTG_OWNER.PS_TRNSFR_SUBJ MODIFY 
  TRNSFR_SRC_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_TRNSFR_SUBJ MODIFY 
  TRNSFR_SRC_ID NOT NULL
  ENABLE VALIDATE
/
