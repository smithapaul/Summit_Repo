ALTER TABLE CSSTG_OWNER.PS_ADDR_ORDER_TBL MODIFY 
  SRC_SYS_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_ADDR_ORDER_TBL MODIFY 
  SRC_SYS_ID NOT NULL
  ENABLE VALIDATE
/
