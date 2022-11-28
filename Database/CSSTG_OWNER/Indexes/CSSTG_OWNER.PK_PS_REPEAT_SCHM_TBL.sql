DROP INDEX CSSTG_OWNER.PK_PS_REPEAT_SCHM_TBL
/

--
-- PK_PS_REPEAT_SCHM_TBL  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_REPEAT_SCHM_TBL ON CSSTG_OWNER.PS_REPEAT_SCHM_TBL
(SETID, REPEAT_SCHEME, EFFDT, SRC_SYS_ID)
/
