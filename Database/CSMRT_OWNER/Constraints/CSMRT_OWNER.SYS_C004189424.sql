ALTER TABLE CSMRT_OWNER.UM_F_CLASS_ENRLMT MODIFY 
  ENRL_REQ_SOURCE NULL
/

ALTER TABLE CSMRT_OWNER.UM_F_CLASS_ENRLMT MODIFY 
  ENRL_REQ_SOURCE NOT NULL
  ENABLE VALIDATE
/
