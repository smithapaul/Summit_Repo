ALTER TABLE CSSTG_OWNER.PS_ISIR_COMPUTED MODIFY 
  SFA_PRIMARY_SCA NULL
/

ALTER TABLE CSSTG_OWNER.PS_ISIR_COMPUTED MODIFY 
  SFA_PRIMARY_SCA NOT NULL
  ENABLE VALIDATE
/
