DROP VIEW CSMRT_OWNER.C_UM_D_PERSON_NAME_VW
/

--
-- C_UM_D_PERSON_NAME_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_UM_D_PERSON_NAME_VW
BEQUEATH DEFINER
AS 
SELECT CAST(ROWNUM AS NUMBER(10))           PERSON_NAME_SID, 
           PERSON_ID                            EMPLID, 
           NAME_TYPE, 
           SRC_SYS_ID, 
           PERSON_SID, 
           EFFDT, 
           EFF_STATUS, 
           cast('001' AS VARCHAR2(3))            COUNTRY_NM_FORMAT, 
           NAME, 
           NAME_INITIALS, 
           NAME_PREFIX, 
           NAME_SUFFIX, 
           cast('-' AS VARCHAR2(15))             NAME_ROYAL_PREFIX, 
           cast('-' AS VARCHAR2(15))             NAME_ROYAL_SUFFIX, 
           NAME_TITLE, 
           LAST_NAME_SRCH, 
           FIRST_NAME_SRCH, 
           LAST_NAME, 
           FIRST_NAME, 
           MIDDLE_NAME, 
           cast('-' AS VARCHAR2(30))             SECOND_LAST_NAME, 
           cast('-' AS VARCHAR2(30))             SECOND_LAST_SRCH, 
           cast('-' AS VARCHAR2(50))             NAME_AC, 
           PREF_FIRST_NAME, 
           cast('-' AS VARCHAR2(30))             PARTNER_LAST_NAME, 
           cast('-' AS VARCHAR2(15))             PARTNER_ROY_PREFIX, 
           cast('1' AS VARCHAR2(1))              LAST_NAME_PREF_NLD, 
           NAME_DISPLAY, 
           NAME_FORMAL, 
           LASTUPDDTTM, 
           LASTUPDOPRID, 
           CAST('N' AS VARCHAR2(1))              LOAD_ERROR, 
           DATA_ORIGIN, 
           CREATED_EW_DTTM, 
           LASTUPD_EW_DTTM, 
           CAST(1234 AS NUMBER(10))              BATCH_SID
	 FROM  CSMRT_OWNER.UM_D_PERSON_NAME
	WHERE DATA_ORIGIN <> 'D'
  ORDER BY PERSON_ID, NAME_TYPE, SRC_SYS_ID
/
