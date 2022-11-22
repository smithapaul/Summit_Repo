DROP VIEW CSMRT_OWNER.UM_D_PERSON_ACCOM_AGG_VW
/

--
-- UM_D_PERSON_ACCOM_AGG_VW  (View) 
--
CREATE OR REPLACE VIEW CSMRT_OWNER.UM_D_PERSON_ACCOM_AGG_VW
BEQUEATH DEFINER
AS 
WITH A1
        AS (SELECT /*+ OPT_ESTIMATE(TABLE UM_D_PERSON_ACCOM MIN=10000) */
                   PERSON_SID,
                   EMPLID,
                   EMPL_RCD,
                   ACCOMMODATION_ID,
                   ACCOMMODATION_OPT,
                   SRC_SYS_ID,
                   INSTITUTION_CD,
                   ACCOM_STATUS,
                   ACCOMMODATION_TYPE,
                   ACCOMMODATION_TYPE_LD,
                   ROW_NUMBER ()
                   OVER (
                      PARTITION BY PERSON_SID,
                                   EMPLID,
                                   EMPL_RCD,
                                   SRC_SYS_ID
                      ORDER BY
                         (CASE WHEN ACCOM_STATUS <> 'A' THEN 99 ELSE 1 END),
                         ACCOMMODATION_ID,
                         ACCOMMODATION_OPT)
                      ACCOM_ORDER
              FROM UM_D_PERSON_ACCOM
             WHERE DATA_ORIGIN <> 'D'  --               AND ACCOM_STATUS = 'A'
               and ROWNUM < 100000
                                     )
     SELECT A1.PERSON_SID,
            A1.EMPLID,
            A1.EMPL_RCD,
            A1.SRC_SYS_ID,
            INSTITUTION_CD,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 1 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_01,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 1 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_01,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 2 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_02,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 2 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_02,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 3 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_03,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 3 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_03,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 4 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_04,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 4 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_04,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 5 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_05,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 5 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_05,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 6 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_06,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 6 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_06,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 7 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_07,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 7 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_07,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 8 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_08,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 8 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_08,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 9 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_09,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 9 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_09,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 10 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_10,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 10 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_10,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 11 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_11,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 11 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_11,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 12 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_12,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 12 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_12,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 13 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_13,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 13 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_13,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 14 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_14,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 14 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_14,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 15 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_15,
            MAX (
               CASE
                  WHEN ACCOM_ORDER = 15 AND ACCOM_STATUS = 'A'
                  THEN
                     ACCOMMODATION_TYPE_LD
                  ELSE
                     NULL
               END)
               ACCOMMODATION_TYPE_LD_15
       FROM A1
   GROUP BY A1.PERSON_SID,
            A1.EMPLID,
            A1.EMPL_RCD,
            A1.SRC_SYS_ID,
            INSTITUTION_CD
/
