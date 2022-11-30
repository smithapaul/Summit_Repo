DROP VIEW DLMRT_OWNER.DL_CIP_UMPO_STEM
/

--
-- DL_CIP_UMPO_STEM  (View) 
--
CREATE OR REPLACE VIEW DLMRT_OWNER.DL_CIP_UMPO_STEM
BEQUEATH DEFINER
AS 
SELECT CIP_CODE,
           CIP_DESCR,
           LAST_UPDATE_BY,
           LAST_UPDATE_TIME
      FROM DLMRT_OWNER.CIP_UMPO_STEM
/
