DROP VIEW CSMRT_OWNER.C_UM_D_PERSON_CS_VW
/

--
-- C_UM_D_PERSON_CS_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_UM_D_PERSON_CS_VW
BEQUEATH DEFINER
AS 
SELECT PERSON_SID,
           PERSON_ID,
           SRC_SYS_ID,
           PERSON_NM,
           FIRST_NM,
           MIDDLE_NM,
           LAST_NM,
           PREFIX,
           SUFFIX,
           PRF_NAME AS PREFERRED_NAME,
           AK1_NAME,
           AK1_FIRST_NAME,
           AK1_MIDDLE_NAME,
           AK1_LAST_NAME,
           AK1_PREFIX,
           AK1_SUFFIX,
           DEG_NAME,
           DEG_FIRST_NAME,
           DEG_MIDDLE_NAME,
           DEG_LAST_NAME,
           DEG_PREFIX,
           DEG_SUFFIX,
           CAST ('-' AS VARCHAR2 (1))
               ATHLETE_FLAG,
           BIRTH_DT,
           BIRTH_PLACE,
           BIRTH_STATE,
           BIRTH_STATE_LD,
           BIRTH_COUNTRY,
           BIRTH_COUNTRY_SD,
           BIRTH_COUNTRY_LD,
           BIRTH_COUNTRY_2CHAR,
           BIRTH_COUNTRY_EU_MEMBER,
           CITZ_STAT_CD,
           CITZ_STAT_SD,
           CITZ_STAT_LD,
           CITZ_CNTRY_CD1,
           CITZ_CNTRY_SD1,
           CITZ_CNTRY_LD1,
           CITZ_CNTRY_2CHAR1,
           CITZ_CNTRY_EU_MEMBER1,
           CITZ_CNTRY_CD2,
           CITZ_CNTRY_SD2,
           CITZ_CNTRY_LD2,
           CITZ_CNTRY_2CHAR2,
           CITZ_CNTRY_EU_MEMBER2,
           CITZ_CNTRY_CD3,
           CITZ_CNTRY_SD3,
           CITZ_CNTRY_LD3,
           CITZ_CNTRY_2CHAR3,
           CITZ_CNTRY_EU_MEMBER3,
           DEATH_DT,
           DEATH_FLG,
           DEATH_PLACE,
           EMERG_CNTCT_NM,
           EMERG_RELATIONSHIP,
           EMERG_RELATIONSHIP_SD,
           EMERG_RELATIONSHIP_LD,
           EMERG_ADDR1_LD,
           EMERG_ADDR2_LD,
           EMERG_ADDR3_LD,
           EMERG_ADDR4_LD,
           EMERG_CITY_NM,
           EMERG_STATE_CD,
           EMERG_POSTAL_CD,
           EMERG_CNTRY_CD,
           EMERG_COUNTRY_CODE,
           EMERG_PHONE_NUM,
           ETHNIC_GRP_FED_SID,
           ETHNIC_GRP_ST_SID,
           FERPA_FLG,
           GENDER_CD,
           GENDER_SD,
           GENDER_LD,
           HI_EDU_LVL_CD,
           HI_EDU_LVL_SD,
           HI_EDU_LVL_LD,
           INTER_STDNT_IND,
           MAR_STAT_CD,
           MAR_STAT_SD,
           MAR_STAT_LD,
           MAR_STAT_DT,
           MIL_STAT_CD,
           MIL_STAT_SD,
           MIL_STAT_LD,
           NTNL_ID,
           NTNL_ID_ERR_CHK,
           MASKED_NTNL_ID,
           CAST ('N' AS VARCHAR2 (5))
               OFF_RESIDENCY,
           CAST ('N' AS VARCHAR2 (30))
               OFF_RESIDENCY_DESC,
           TO_DATE ('1/1/1900', 'MM/DD/YYYY')
               OFF_RESIDENCY_DT,
           CAST ('N' AS VARCHAR2 (30))
               OFF_CITY,
           CAST ('N' AS VARCHAR2 (30))
               OFF_COUNTY,
           CAST ('N' AS VARCHAR2 (6))
               OFF_STATE,
           CAST ('N' AS VARCHAR2 (3))
               OFF_COUNTRY,
           CAST ('N' AS VARCHAR2 (16))
               STDNT_CAMPUS_ID,
           US_WORK_ELIG_IND,
           VA_BENEFIT,
           VETERAN_STATUS_UMBOS,
           VETERAN_STATUS_UMDAR,
           VETERAN_STATUS_UMLOW,
           VISA_PERMIT_TYPE,
           VISA_PERMT_TY_DESC,
           VISA_EFFDT,
           VISA_WRKPMT_STATUS,
           VISA_WRKPMT_STATUS_SD,
           VISA_WRKPMT_STATUS_LD,
           HOME_ADDR1_LD,
           HOME_ADDR2_LD,
           HOME_ADDR3_LD,
           HOME_ADDR4_LD,
           HOME_CITY_NM,
           HOME_STATE_CD,
           HOME_POSTAL_CD,
           HOME_CNTRY_CD,
           HOME_CNTRY_SD,
           HOME_CNTRY_LD,
           HOME_CNTRY_2CHAR,
           HOME_CNTRY_EU_MEMBER,
           CAST (HOME_UMLOW_GRAD_PROXIMITY AS VARCHAR2 (6))
               HOME_UMLOW_GRAD_PROXIMITY,
           CAST (HOME_UMLOW_UGRD_PROXIMITY AS VARCHAR2 (6))
               HOME_UMLOW_UGRD_PROXIMITY,
           PERM_STATE_CD,
           PERM_CNTRY_CD,
           PERM_CNTRY_SD,
           PERM_CNTRY_LD,
           PERM_CNTRY_2CHAR,
           PERM_CNTRY_EU_MEMBER,
           STDNT_EMAIL_UMBOS,
           STDNT_EMAIL_UMDAR,
           STDNT_EMAIL_UMLOW,
           EMPL_EMAIL_UMBOS,
           EMPL_EMAIL_UMDAR,
           EMPL_EMAIL_UMLOW,
           PERS_EMAIL,
           OTHER_EMAIL,
           HOME_PHONE_NUM,
           CELL_PHONE_NUM,
           WORK_PHONE_NUM,
           PREF_PHONE_TYPE,
           PREF_COUNTRY_CODE,
           PREF_PHONE_NUM,
           CAST ('' AS VARCHAR2 (3))
               EG_ACADEMIC_RANK,
           CAST ('' AS VARCHAR2 (30))
               EG_ACADEMIC_RANK_LD,
           CAST ('-' AS VARCHAR2 (3))
               TENURE_STATUS,
           CAST ('-' AS VARCHAR2 (30))
               TENURE_STATUS_LD,
           CAST ('' AS VARCHAR2 (10))
               DEPT_ID_TENURE_HOME,
           CAST (2147483646 AS INTEGER)
               DEPT_SID_TENURE_HOME,
           CAST ('' AS VARCHAR2 (8))
               HR_POSITION_NBR,
           CAST (2147483646 AS INTEGER)
               HR_DEPT_SID,
           --           CAST ('-' AS VARCHAR2 (15))
           HR_PERSON_ID,
           CAST ('' AS VARCHAR2 (1))
               HR_REG_TEMP,
           CAST ('' AS VARCHAR2 (1))
               HR_FULL_PART_TIME,
           CAST ('' AS VARCHAR2 (1))
               HR_EMPL_TYPE,
           CAST (2147483646 AS INTEGER)
               HR_ETHNIC_GRP_SID,
           CAST ('' AS VARCHAR2 (6))
               HR_JOBCODE,
           CAST ('' AS VARCHAR2 (10))
               HR_JOBCODE_SD,
           CAST ('' AS VARCHAR2 (30))
               HR_JOBCODE_LD,
           CAST ('' AS VARCHAR2 (1))
               HR_IPEDSSCODE,
           CAST ('' AS VARCHAR2 (10))
               HR_IPEDSSCODE_SD,
           CAST ('' AS VARCHAR2 (30))
               HR_IPEDSSCODE_LD,
           SEVIS_ID,
		   UNDER_MINORITY_FLAG,   -- 22168 Added Underrepresented Minority Flag
           ITIN,
           CAST ('N' AS VARCHAR2 (1))
               LOAD_ERROR,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM,
           CAST (1234 AS NUMBER (10))
               BATCH_SID
      FROM CSMRT_OWNER.UM_D_PERSON_AGG
/
