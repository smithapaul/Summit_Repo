ALTER TABLE CSSTG_OWNER.PS_HRS_APP_PROFILE_OLD MODIFY 
  DESIRED_SHIFT NULL
/

ALTER TABLE CSSTG_OWNER.PS_HRS_APP_PROFILE_OLD MODIFY 
  DESIRED_SHIFT NOT NULL
  ENABLE VALIDATE
/
