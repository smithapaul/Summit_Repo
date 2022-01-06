CREATE OR REPLACE VIEW C_UM_F_EXT_DEG_VW
BEQUEATH DEFINER
AS 
SELECT CAST (PERSON_SID AS NUMBER (10))
               PERSON_SID,
           CAST (EXT_ORG_SID AS NUMBER (10))
               EXT_ORG_SID,
           EXT_DEG_NBR,
           SRC_SYS_ID,
           PERSON_ID
               EMPLID,
           EXT_ORG_ID,
           DESCR,
           CAST (EXT_DEG_SID AS NUMBER (10))
               EXT_DEG_SID,
           CAST (EXT_DATA_SRC_SID AS NUMBER (10))
               EXT_DATA_SRC_SID,
           CAST (EXT_SUBJECT_AREA_SID_1 AS NUMBER (10))
               EXT_SUBJECT_AREA_SID_1,
           CAST (EXT_SUBJECT_AREA_SID_2 AS NUMBER (10))
               EXT_SUBJECT_AREA_SID_2,
           EXT_CAREER,
           EXT_CAREER_SD,
           EXT_CAREER_LD,
           EXT_DATA_NBR,
           EXT_DEG_DT,
           EXT_DEG_STAT_ID,
           EXT_DEG_STAT_SD,
           EXT_DEG_STAT_LD,
           FIELD_OF_STUDY_1,
           FIELD_OF_STUDY_2,
           HONORS_CATEGORY,
           HONORS_CATEGORY_SD,
           HONORS_CATEGORY_LD,
           CAST ('N' AS VARCHAR2 (1))
               LOAD_ERROR,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM,
           CAST (1234 AS NUMBER (10))
               BATCH_SID
      FROM CSMRT_OWNER.UM_F_EXT_DEG;
