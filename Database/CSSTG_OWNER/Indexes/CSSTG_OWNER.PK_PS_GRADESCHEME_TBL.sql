DROP INDEX CSSTG_OWNER.PK_PS_GRADESCHEME_TBL
/

--
-- PK_PS_GRADESCHEME_TBL  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_GRADESCHEME_TBL ON CSSTG_OWNER.PS_GRADESCHEME_TBL
(SETID, GRADING_SCHEME, EFFDT, SRC_SYS_ID)
/
