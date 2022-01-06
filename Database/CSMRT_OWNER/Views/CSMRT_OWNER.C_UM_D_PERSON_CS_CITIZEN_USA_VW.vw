CREATE OR REPLACE VIEW C_UM_D_PERSON_CS_CITIZEN_USA_VW
BEQUEATH DEFINER
AS 
SELECT CAST (PERSON_SID AS NUMBER (10))
               PERSON_SID,
           CAST (PERSON_ID AS VARCHAR2 (15))
               EMPLID,
           SRC_SYS_ID,
           CASE
               WHEN COUNTRY = 'USA' OR CITIZENSHIP_STATUS_USA IN ('1', '2')
               THEN
                   '-'
               ELSE
                   COUNTRY
           END
               COUNTRY,
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
           CAST ('-' AS VARCHAR2 (2))
               WORKER_TYPE_SGP,
           TO_DATE ('1/1/1900', 'MM/DD/YYYY')
               PERM_STATUS_DT_SGP
      FROM CSMRT_OWNER.UM_D_PERSON_CITIZEN
     WHERE CIT_ORDER = 1;
