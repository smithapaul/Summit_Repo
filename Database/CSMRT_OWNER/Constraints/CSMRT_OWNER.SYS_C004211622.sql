ALTER TABLE CSMRT_OWNER.UM_D_CLASS_BKP MODIFY 
  SSR_DROP_CONSENT NULL
/

ALTER TABLE CSMRT_OWNER.UM_D_CLASS_BKP MODIFY 
  SSR_DROP_CONSENT NOT NULL
  ENABLE VALIDATE
/
