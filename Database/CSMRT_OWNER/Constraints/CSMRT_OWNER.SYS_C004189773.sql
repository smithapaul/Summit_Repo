ALTER TABLE CSMRT_OWNER.UM_F_CLASS_PERM MODIFY 
  OVRD_CLASS_LIMIT NULL
/

ALTER TABLE CSMRT_OWNER.UM_F_CLASS_PERM MODIFY 
  OVRD_CLASS_LIMIT NOT NULL
  ENABLE VALIDATE
/
