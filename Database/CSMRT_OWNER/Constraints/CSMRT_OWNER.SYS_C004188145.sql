ALTER TABLE CSMRT_OWNER.UM_D_ITEM_LINE MODIFY 
  SRC_SYS_ID NULL
/

ALTER TABLE CSMRT_OWNER.UM_D_ITEM_LINE MODIFY 
  SRC_SYS_ID NOT NULL
  ENABLE VALIDATE
/
