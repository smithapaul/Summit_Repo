DROP VIEW CSMRT_OWNER.UM_D_PERSON_NAME_VW
/

--
-- UM_D_PERSON_NAME_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_PERSON_NAME_VW
BEQUEATH DEFINER
AS 
SELECT PERSON_ID,
           NAME_TYPE,
           SRC_SYS_ID,
           EFFDT,
           EFF_STATUS,
           PERSON_SID,
           PERSON_ID     EMPLID,
           '001'         COUNTRY_NM_FORMAT,               -- Still used in RPD
           NAME,
           NAME_INITIALS,
           NAME_PREFIX,
           NAME_SUFFIX,
           NAME_TITLE,
           LAST_NAME_SRCH,
           FIRST_NAME_SRCH,
           LAST_NAME,
           FIRST_NAME,
           MIDDLE_NAME,
           PREF_FIRST_NAME,
           NAME_DISPLAY,
           NAME_FORMAL,
           LAST_NAME_FORMER,
           NAME_FORMER,
           LASTUPDDTTM,
           LASTUPDOPRID,
           NAME_ORDER,
           AKA_ORDER,
           CPS_ORDER,       -- Nov 2020
           DEG_ORDER,
           PRF_ORDER,
           PRI_ORDER,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM UM_D_PERSON_NAME
/
