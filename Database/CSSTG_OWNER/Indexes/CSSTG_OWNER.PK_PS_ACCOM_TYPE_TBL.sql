DROP INDEX CSSTG_OWNER.PK_PS_ACCOM_TYPE_TBL
/

--
-- PK_PS_ACCOM_TYPE_TBL  (Index) 
--
CREATE UNIQUE INDEX CSSTG_OWNER.PK_PS_ACCOM_TYPE_TBL ON CSSTG_OWNER.PS_ACCOM_TYPE_TBL
(ACCOMMODATION_TYPE, EFFDT, SRC_SYS_ID)
/
