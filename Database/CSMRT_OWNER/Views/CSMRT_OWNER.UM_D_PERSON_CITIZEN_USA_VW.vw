DROP VIEW CSMRT_OWNER.UM_D_PERSON_CITIZEN_USA_VW
/

--
-- UM_D_PERSON_CITIZEN_USA_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_PERSON_CITIZEN_USA_VW
BEQUEATH DEFINER
AS 
SELECT PERSON_ID,
           DEPENDENT_ID,
           CASE
               WHEN COUNTRY = 'USA' OR CITIZENSHIP_STATUS_USA IN ('1', '2')
               THEN
                   '-'
               ELSE
                   COUNTRY
           END
               COUNTRY,
           SRC_SYS_ID,
           PERSON_ID
               EMPLID,
           PERSON_SID,
           CASE
               WHEN COUNTRY = 'USA' OR CITIZENSHIP_STATUS_USA IN ('1', '2')
               THEN
                   '-'
               ELSE
                   COUNTRY_SD
           END
               COUNTRY_SD,
           CASE
               WHEN COUNTRY = 'USA' OR CITIZENSHIP_STATUS_USA IN ('1', '2')
               THEN
                   '-'
               ELSE
                   COUNTRY_LD
           END
               COUNTRY_LD,
           CASE
               WHEN COUNTRY = 'USA' OR CITIZENSHIP_STATUS_USA IN ('1', '2')
               THEN
                   '-'
               ELSE
                   COUNTRY_2CHAR
           END
               COUNTRY_2CHAR,
           CASE
               WHEN COUNTRY = 'USA' OR CITIZENSHIP_STATUS_USA IN ('1', '2')
               THEN
                   '-'
               ELSE
                   EU_MEMBER_STATE
           END
               EU_MEMBER_STATE,
           CITIZENSHIP_STATUS_USA
               CITIZENSHIP_STATUS,
           CITIZENSHIP_STATUS_SD_USA
               CITIZENSHIP_STATUS_SD,
           CITIZENSHIP_STATUS_LD_USA
               CITIZENSHIP_STATUS_LD,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM UM_D_PERSON_CITIZEN
     WHERE CIT_ORDER = 1
/
