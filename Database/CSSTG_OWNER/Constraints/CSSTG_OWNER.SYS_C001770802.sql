ALTER TABLE CSSTG_OWNER.PS_GL_INTERFACE MODIFY 
  DEFERRED_DEPTID NULL
/

ALTER TABLE CSSTG_OWNER.PS_GL_INTERFACE MODIFY 
  DEFERRED_DEPTID NOT NULL
  ENABLE VALIDATE
/
