ALTER TABLE CSMRT_OWNER.PS_D_ENRL_HDR_STAT MODIFY 
  HDR_STATUS_SD NULL
/

ALTER TABLE CSMRT_OWNER.PS_D_ENRL_HDR_STAT MODIFY 
  HDR_STATUS_SD NOT NULL
  ENABLE VALIDATE
/
