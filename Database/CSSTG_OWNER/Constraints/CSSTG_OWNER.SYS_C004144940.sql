ALTER TABLE CSSTG_OWNER.PS_T_EVENT_TYPE_TBL MODIFY 
  CAMPUS_EVENT_TYPE NULL
/

ALTER TABLE CSSTG_OWNER.PS_T_EVENT_TYPE_TBL MODIFY 
  CAMPUS_EVENT_TYPE NOT NULL
  ENABLE VALIDATE
/
