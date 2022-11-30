DROP VIEW DLMRT_OWNER.DL_CIP
/

--
-- DL_CIP  (View) 
--
CREATE OR REPLACE VIEW DLMRT_OWNER.DL_CIP
BEQUEATH DEFINER
AS 
SELECT CIP_CODE,
           CIP_YEAR,
           CIP_DESCR,
           CIP_DESCR254,
           UMPO_STEM,
           CIP_CODE_FAMILY,
           CIP_DESCR_FAMILY,
           CIP_DESCR254_FAMILY,
           CIP_CODE_DISCIPLINE,
           CIP_DESCR_DISCIPLINE,
           CIP_DESCR254_DISCIPLINE,
           INSERT_TIME
      FROM DLMRT_OWNER.CIP
/
