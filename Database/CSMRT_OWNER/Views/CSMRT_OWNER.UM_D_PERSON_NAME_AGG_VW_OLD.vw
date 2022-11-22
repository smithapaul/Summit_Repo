DROP VIEW CSMRT_OWNER.UM_D_PERSON_NAME_AGG_VW_OLD
/

--
-- UM_D_PERSON_NAME_AGG_VW_OLD  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_PERSON_NAME_AGG_VW_OLD
BEQUEATH DEFINER
AS 
SELECT PERSON_ID,
           SRC_SYS_ID,
           PERSON_SID,
           PRI_NAME,
           PRI_FIRST_NAME,
           PRI_MIDDLE_NAME,
           PRI_LAST_NAME,
           PRI_PREFIX,
           PRI_SUFFIX,
           DEG_NAME,
           DEG_FIRST_NAME,
           DEG_MIDDLE_NAME,
           DEG_LAST_NAME,
           DEG_PREFIX,
           DEG_SUFFIX,
           PRF_NAME,
           PRF_FIRST_NAME,
           PRF_MIDDLE_NAME,
           PRF_LAST_NAME,
           PRF_PREFIX,
           PRF_SUFFIX,
           AKA_NAME,
           AKA_FIRST_NAME,
           AKA_MIDDLE_NAME,
           AKA_LAST_NAME,
           AKA_PREFIX,
           AKA_SUFFIX,
           CPS_NAME,        -- Nov 2020
           CPS_FIRST_NAME,  -- Nov 2020
           CPS_MIDDLE_NAME, -- Nov 2020
           CPS_LAST_NAME,   -- Nov 2020
           CPS_PREFIX,      -- Nov 2020
           CPS_SUFFIX,      -- Nov 2020
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM UM_D_PERSON_NAME_AGG
/
