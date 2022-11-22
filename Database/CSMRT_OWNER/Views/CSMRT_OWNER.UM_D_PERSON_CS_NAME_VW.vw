DROP VIEW CSMRT_OWNER.UM_D_PERSON_CS_NAME_VW
/

--
-- UM_D_PERSON_CS_NAME_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_PERSON_CS_NAME_VW
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
           --           NAME_ROYAL_PREFIX,
           --           NAME_ROYAL_SUFFIX,
           NAME_TITLE,
           LAST_NAME_SRCH,
           FIRST_NAME_SRCH,
           LAST_NAME,
           FIRST_NAME,
           MIDDLE_NAME,
           --           SECOND_LAST_NAME,
           --           SECOND_LAST_SRCH,
           --           NAME_AC,
           PREF_FIRST_NAME,
           --           PARTNER_LAST_NAME,
           --           PARTNER_ROY_PREFIX,
           --           LAST_NAME_PREF_NLD,
           NAME_DISPLAY,
           NAME_FORMAL,
           --           NVL (
           --               (SELECT LAST_NAME
           --                  FROM UM_D_PERSON_NAME N2
           --                 WHERE     N.PERSON_SID = N2.PERSON_SID
           --                       AND N.NAME_TYPE NOT LIKE 'AK%'
           --                       AND N2.NAME_TYPE = 'AK1'),
           --               '-')
           --               LAST_NAME_FORMER,
           --           NVL (
           --               (SELECT NAME
           --                  FROM UM_D_PERSON_NAME N2
           --                 WHERE     N.PERSON_SID = N2.PERSON_SID
           --                       AND N.NAME_TYPE NOT LIKE 'AK%'
           --                       AND N2.NAME_TYPE = 'AK1'),
           --               '-')
           --               NAME_FORMER,
           LASTUPDDTTM,
           LASTUPDOPRID,
           NAME_ORDER,
           AKA_ORDER,
           --           LOAD_ERROR,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      --           BATCH_SID
      --      FROM UM_D_PERSON_NAME_AGG N
      FROM UM_D_PERSON_NAME N
     WHERE DATA_ORIGIN <> 'D'
/
