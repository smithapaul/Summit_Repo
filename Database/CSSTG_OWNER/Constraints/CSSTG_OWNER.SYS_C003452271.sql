ALTER TABLE CSSTG_OWNER.PS_ITEM_TYPE_FISCL MODIFY 
  DECLINED_COUNT NULL
/

ALTER TABLE CSSTG_OWNER.PS_ITEM_TYPE_FISCL MODIFY 
  DECLINED_COUNT NOT NULL
  ENABLE VALIDATE
/
