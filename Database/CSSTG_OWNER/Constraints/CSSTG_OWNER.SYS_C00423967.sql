ALTER TABLE CSSTG_OWNER.PS_HRS_APP_PROFILE_OLD MODIFY 
  GVT_VET_PREF_APPT NULL
/

ALTER TABLE CSSTG_OWNER.PS_HRS_APP_PROFILE_OLD MODIFY 
  GVT_VET_PREF_APPT NOT NULL
  ENABLE VALIDATE
/
