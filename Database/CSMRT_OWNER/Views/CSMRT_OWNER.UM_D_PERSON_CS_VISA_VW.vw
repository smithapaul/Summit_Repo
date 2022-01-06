CREATE OR REPLACE VIEW UM_D_PERSON_CS_VISA_VW
BEQUEATH DEFINER
AS 
SELECT PERSON_SID,
--           NVL (V.PERSON_VISA_SID, 2147483646) PERSON_VISA_SID,
           PERSON_ID,
           PERSON_ID EMPLID,
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
           DATA_ORIGIN,
           CREATED_EW_DTTM,
           LASTUPD_EW_DTTM
      FROM UM_D_PERSON_VISA
     WHERE DATA_ORIGIN <> 'D';
