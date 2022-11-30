DROP TABLE DLMRT_OWNER.CIP_UMPO_STEM CASCADE CONSTRAINTS
/

--
-- CIP_UMPO_STEM  (Table) 
--
CREATE TABLE DLMRT_OWNER.CIP_UMPO_STEM
(
  CIP_CODE          VARCHAR2(7 BYTE),
  CIP_DESCR         VARCHAR2(254 BYTE),
  LAST_UPDATE_BY    VARCHAR2(100 BYTE),
  LAST_UPDATE_TIME  DATE                        DEFAULT SYSDATE
)
NOCOMPRESS
/

COMMENT ON TABLE DLMRT_OWNER.CIP_UMPO_STEM IS 'Contains CIP_CODES that are considered by UMass President''s Office to be STEM.'
/

COMMENT ON COLUMN DLMRT_OWNER.CIP_UMPO_STEM.CIP_DESCR IS 'Descriptions in this table are for convenience.  They are not the source of CIP description data.'
/
