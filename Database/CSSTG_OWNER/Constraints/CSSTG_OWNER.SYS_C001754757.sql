ALTER TABLE CSSTG_OWNER.PS_T_ITEM_LINE_SF MODIFY 
  PAYMENT_ID_NBR NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_ITEM_LINE_SF MODIFY 
  PAYMENT_ID_NBR NOT NULL
  ENABLE VALIDATE
/
