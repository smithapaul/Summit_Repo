ALTER TABLE CSSTG_OWNER.PS_BUDGET_ITEM MODIFY 
  LOAD_ERROR NULL
/

ALTER TABLE CSSTG_OWNER.PS_BUDGET_ITEM MODIFY 
  LOAD_ERROR NOT NULL
  ENABLE VALIDATE
/
