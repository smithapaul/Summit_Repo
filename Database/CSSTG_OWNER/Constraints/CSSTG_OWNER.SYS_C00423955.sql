ALTER TABLE CSSTG_OWNER.PS_HRS_APP_PROFILE_OLD MODIFY 
  HRS_RESUME_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_HRS_APP_PROFILE_OLD MODIFY 
  HRS_RESUME_ID NOT NULL
  ENABLE VALIDATE
/
