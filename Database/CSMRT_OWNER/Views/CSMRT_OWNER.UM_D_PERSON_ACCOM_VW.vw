CREATE OR REPLACE VIEW UM_D_PERSON_ACCOM_VW
BEQUEATH DEFINER
AS 
WITH A2
        AS (SELECT /*+ OPT_ESTIMATE(TABLE UM_D_PERSON_ACCOM MIN=10000) */
                   PERSON_SID,
                   EMPLID,
                   EMPL_RCD,
                   ACCOMMODATION_ID,
                   ACCOMMODATION_OPT,
                   SRC_SYS_ID,
                   ROW_NUMBER ()
                      OVER (PARTITION BY PERSON_SID,
                                         EMPLID,
                                         EMPL_RCD,
                                         SRC_SYS_ID
                            ORDER BY ACCOMMODATION_ID, ACCOMMODATION_OPT)
                      ACCOM_ORDER
              FROM UM_D_PERSON_ACCOM
             WHERE DATA_ORIGIN <> 'D' 
               AND ACCOM_STATUS = 'A'
               and ROWNUM < 100000
             )
   SELECT /*+ OPT_ESTIMATE(TABLE UM_D_PERSON_ACCOM MIN=10000) */
          A1.PERSON_SID,
          A1.EMPLID,
          A1.EMPL_RCD,
          A1.ACCOMMODATION_ID,
          A1.ACCOMMODATION_OPT,
          A1.SRC_SYS_ID,
          INSTITUTION_CD,
          PERSON_RESP_SID,
          DT_REQUESTED,
          REQUEST_STATUS,
          REQUEST_STATUS_SD,
          REQUEST_STATUS_LD,
          REQ_STATUS_DT,
          ACCOMMODATION_TYPE,
          ACCOMMODATION_TYPE_SD,
          ACCOMMODATION_TYPE_LD,
          ACCOM_STATUS,
          ACCOM_STATUS_SD,
          ACCOM_STATUS_LD,
          ACCOM_STATUS_DT,
          NVL (ACCOM_ORDER, 0) ACCOM_ORDER,
          REQ_COMMENTS,
          ACCOM_DESCRLONG,
          LOAD_ERROR,
          DATA_ORIGIN,
          CREATED_EW_DTTM,
          LASTUPD_EW_DTTM,
          BATCH_SID
     FROM UM_D_PERSON_ACCOM A1
     LEFT OUTER JOIN A2
       ON A1.PERSON_SID = A2.PERSON_SID
      AND A1.EMPLID = A2.EMPLID
      AND A1.EMPL_RCD = A2.EMPL_RCD
      AND A1.ACCOMMODATION_ID = A2.ACCOMMODATION_ID
      AND A1.ACCOMMODATION_OPT = A2.ACCOMMODATION_OPT
      AND A1.SRC_SYS_ID = A2.SRC_SYS_ID
    where ROWNUM < 100000;
