ALTER TABLE CSMRT_OWNER.PS_D_DEG_STAT MODIFY 
  DEG_STAT_SD NULL
/

ALTER TABLE CSMRT_OWNER.PS_D_DEG_STAT MODIFY 
  DEG_STAT_SD NOT NULL
  ENABLE VALIDATE
/
