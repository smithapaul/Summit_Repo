DROP INDEX CSSTG_OWNER.PK_PS_SUBJECT_TBL
/

--
-- PK_PS_SUBJECT_TBL  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_SUBJECT_TBL ON CSSTG_OWNER.PS_SUBJECT_TBL
(INSTITUTION, SUBJECT, EFFDT, SRC_SYS_ID)
/
