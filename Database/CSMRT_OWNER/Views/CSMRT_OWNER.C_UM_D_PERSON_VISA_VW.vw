DROP VIEW CSMRT_OWNER.C_UM_D_PERSON_VISA_VW
/

--
-- C_UM_D_PERSON_VISA_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.C_UM_D_PERSON_VISA_VW
BEQUEATH DEFINER
AS 
SELECT CAST (PERSON_SID AS NUMBER (10))     PERSON_SID,
           CAST (ROWNUM AS NUMBER (10))         PERSON_VISA_SID,
           PERSON_ID                            EMPLID,
           DEPENDENT_ID,
           COUNTRY,
           VISA_PERMIT_TYPE,
           SRC_SYS_ID,
           VISA_PERMIT_SID,
           EFFDT,
           VISA_WRKPMT_NBR,
           VISA_WRKPMT_STATUS,
           VISA_WRKPMT_STATUS_SD,
           VISA_WRKPMT_STATUS_LD,
           STATUS_DT,
           DT_ISSUED,
           PLACE_ISSUED,
           DURATION_TIME,
           DURATION_TYPE,
           DURATION_TYPE_SD,
           DURATION_TYPE_LD,
           ENTRY_DT,
           EXPIRATN_DT,
           ISSUING_AUTHORITY,
           VISA_ORDER,
           CAST ('N' AS VARCHAR2 (1))           LOAD_ERROR,
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM,
           CAST (1234 AS NUMBER (10))           BATCH_SID
      FROM CSMRT_OWNER.UM_D_PERSON_VISA
/
