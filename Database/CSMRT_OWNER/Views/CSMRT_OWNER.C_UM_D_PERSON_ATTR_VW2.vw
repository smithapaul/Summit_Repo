CREATE OR REPLACE VIEW C_UM_D_PERSON_ATTR_VW2
BEQUEATH DEFINER
AS 
WITH
        RSD
        AS
            (SELECT /*+ inline */
                    EMPLID,
                    SRC_SYS_ID,
                    TUITION_RES,
                    RESIDENCY_DT,
                    CITY,
                    COUNTY,
                    STATE,
                    COUNTRY,
                    ROW_NUMBER ()
                        OVER (
                            PARTITION BY EMPLID, SRC_SYS_ID
                            ORDER BY
                                EFFECTIVE_TERM DESC, ACAD_CAREER, INSTITUTION)
                        RES_ORDER
               FROM CSSTG_OWNER.PS_RESIDENCY_OFF
              WHERE DATA_ORIGIN <> 'D'),
        R
        AS
            (SELECT /*+ inline */
                    RESIDENCY,
                    SRC_SYS_ID,
                    EFFDT,
                    EFF_STATUS,
                    DESCR,
                    DESCRSHORT,
                    ROW_NUMBER ()
                        OVER (
                            PARTITION BY RESIDENCY, SRC_SYS_ID
                            ORDER BY
                                DATA_ORIGIN DESC,
                                (CASE
                                     WHEN EFFDT > TRUNC (SYSDATE)
                                     THEN
                                         TO_DATE ('01-JAN-1900')
                                     ELSE
                                         EFFDT
                                 END) DESC)
                        R_ORDER
               FROM CSSTG_OWNER.PS_RESIDENCY_TBL
              WHERE DATA_ORIGIN <> 'D')
--        VISA
--        AS
--            (SELECT /*+ inline */
--                    EMPLID,
--                    SRC_SYS_ID,
--                    EFFDT,
--                    VISA_PERMIT_TYPE,
--                    STATUS_DT,
--                    VISA_WRKPMT_STATUS,
--                    ROW_NUMBER ()
--                        OVER (
--                            PARTITION BY EMPLID, SRC_SYS_ID
--                            ORDER BY
--                                STATUS_DT DESC, EFFDT DESC, VISA_PERMIT_TYPE)
--                        VISA_ORDER
--               FROM CSSTG_OWNER.PS_VISA_PMT_DATA
--              WHERE DATA_ORIGIN <> 'D'),
--        VP
--        AS
--            (SELECT /*+ inline */
--                    COUNTRY,
--                    VISA_PERMIT_TYPE,
--                    SRC_SYS_ID,
--                    EFFDT,
--                    EFF_STATUS,
--                    VISA_PERMIT_CLASS,
--                    DESCR,
--                    DESCRSHORT,
--                    ROW_NUMBER ()
--                        OVER (
--                            PARTITION BY COUNTRY,
--                                         VISA_PERMIT_TYPE,
--                                         SRC_SYS_ID
--                            ORDER BY
--                                DATA_ORIGIN DESC,
--                                (CASE
--                                     WHEN EFFDT > TRUNC (SYSDATE)
--                                     THEN
--                                         TO_DATE ('01-JAN-1900')
--                                     ELSE
--                                         EFFDT
--                                 END) DESC)
--                        VP_ORDER
--               FROM CSSTG_OWNER.PS_VISA_PERMIT_TBL
--              WHERE DATA_ORIGIN <> 'D')
    SELECT P.PERSON_SID,
           P.PERSON_ID,
           P.SRC_SYS_ID,
--           NVL (CIT.CITIZENSHIP_STATUS, '-')
               CITZ_STAT_CD,
--           NVL (CS.DESCRSHORT, '-')
               CITZ_STAT_SD,
--           NVL (CS.DESCR, '-')
               CITZ_STAT_LD,
--           P.BIRTH_COUNTRY
               CITZ_CNTRY_CD1,                       -- Same as home country???
--           P.BIRTH_COUNTRY_SD
               CITZ_CNTRY_SD1,                       -- Same as home country???
--           P.BIRTH_COUNTRY_LD
               CITZ_CNTRY_LD1,                       -- Same as home country???
--           NVL (VISA.VISA_PERMIT_TYPE, '-')
               VISA_PERMIT_TYPE,
--           NVL (VP.DESCR, '-')
               VISA_PERMT_TY_DESC,
--           NVL (VISA.EFFDT, TO_DATE (TO_CHAR ('01/01/1900'), 'MM/DD/YYYY'))
               VISA_EFFDT,
--           NVL (VISA.VISA_WRKPMT_STATUS, '-')
               VISA_WRKPMT_STATUS,
--           NVL (
--               (SELECT MIN (X.XLATSHORTNAME)
--                  FROM UM_D_XLATITEM_VW X
--                 WHERE     X.FIELDNAME = 'VISA_WRKPMT_STATUS'
--                       AND X.FIELDVALUE = VISA.VISA_WRKPMT_STATUS),
--               '-')
               VISA_WRKPMT_STATUS_SD,
--           NVL (
--               (SELECT MIN (X.XLATLONGNAME)
--                  FROM UM_D_XLATITEM_VW X
--                 WHERE     X.FIELDNAME = 'VISA_WRKPMT_STATUS'
--                       AND X.FIELDVALUE = VISA.VISA_WRKPMT_STATUS),
--               '-')
               VISA_WRKPMT_STATUS_LD,
           NVL (RSD.TUITION_RES, '-')
               OFF_RESIDENCY,
           NVL (R.DESCR, '-')
               OFF_RESIDENCY_DESC,
           RSD.RESIDENCY_DT
               OFF_RESIDENCY_DT,
           NVL (RSD.CITY, '-')
               OFF_CITY,
           NVL (RSD.COUNTY, '-')
               OFF_COUNTY,
           NVL (RSD.STATE, '-')
               OFF_STATE,
           NVL (RSD.COUNTRY, '-')
               OFF_COUNTRY,
           CAST ('-' AS VARCHAR2 (16))
               STDNT_CAMPUS_ID,
           P.FERPA_FLG,
           --       A.CURRENT_ATHL_FLG ATHLETE_FLAG,     -- Correct source???
           '-'
               ATHLETE_FLAG,
           TO_DATE ('01-JAN-1753')
               EFF_START_DT,
           TO_DATE ('31-DEC-9999')
               EFF_END_DT,
           CAST ('Y' AS VARCHAR2 (1))
               CURRENT_IND,
           CAST ('N' AS VARCHAR2 (1))
               LOAD_ERROR,
           P.DATA_ORIGIN,
           P.CREATED_EW_DTTM,
           P.LASTUPD_EW_DTTM,
           1234
               BATCH_SID
      FROM UM_D_PERSON_AGG  P
           --  join UM_R_PERSON_ASSOC A      -- Only for ATHLETE_FLAG???    -- Column depricated in Summit.  Column only used in IR_PERSON in Census.
           --    on P.PERSON_ID = A.PERSON_ID                                  Census gets column from CENSUS_UM_R_PERSON_ASSOC table.
           --   and P.SRC_SYS_ID = A.SRC_SYS_ID
--           LEFT OUTER JOIN CSSTG_OWNER.PS_CITIZENSHIP CIT
--               ON     P.PERSON_ID = CIT.EMPLID
--                  AND CIT.DEPENDENT_ID = '-'
--                  AND CIT.COUNTRY = 'USA'
--                  AND P.SRC_SYS_ID = CIT.SRC_SYS_ID
--                  AND CIT.DATA_ORIGIN <> 'D'
--           LEFT OUTER JOIN CSSTG_OWNER.PS_CITIZEN_STS_TBL CS
--               ON     P.BIRTH_COUNTRY = CS.COUNTRY
--                  AND CIT.CITIZENSHIP_STATUS = CS.CITIZENSHIP_STATUS
--                  AND P.SRC_SYS_ID = CIT.SRC_SYS_ID
--                  AND CS.DATA_ORIGIN <> 'D'
--           LEFT OUTER JOIN VISA
--               ON     P.PERSON_ID = VISA.EMPLID
--                  AND P.SRC_SYS_ID = VISA.SRC_SYS_ID
--                  AND VISA.VISA_ORDER = 1
--           LEFT OUTER JOIN VP
--               ON     VISA.VISA_PERMIT_TYPE = VP.VISA_PERMIT_TYPE
--                  AND P.SRC_SYS_ID = VP.SRC_SYS_ID
--                  AND VP.COUNTRY = 'USA'
--                  AND VP.VP_ORDER = 1
           LEFT OUTER JOIN RSD
               ON     P.PERSON_ID = RSD.EMPLID
                  AND P.SRC_SYS_ID = RSD.SRC_SYS_ID
                  AND RSD.RES_ORDER = 1
           LEFT OUTER JOIN R
               ON     RSD.TUITION_RES = R.RESIDENCY
                  AND P.SRC_SYS_ID = R.SRC_SYS_ID
                  AND R.R_ORDER = 1;
