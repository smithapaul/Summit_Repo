ALTER TABLE CSSTG_OWNER.PS_SSR_STDGRD_HIST MODIFY 
  SRC_SYS_ID NULL
/

ALTER TABLE CSSTG_OWNER.PS_SSR_STDGRD_HIST MODIFY 
  SRC_SYS_ID NOT NULL
  ENABLE VALIDATE
/
