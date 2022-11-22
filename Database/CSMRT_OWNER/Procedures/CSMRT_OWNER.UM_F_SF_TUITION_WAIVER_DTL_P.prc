DROP PROCEDURE CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL_P
/

--
-- UM_F_SF_TUITION_WAIVER_DTL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_SF_TUITION_WAIVER_DTL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads table UM_F_SF_TUITION_WAIVER_DTL.
--
--V01   SMT-xxxx 01/14/2019,    James Doucette
--                              Converted from Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_SF_TUITION_WAIVER_DTL';
        intProcessSid                   Integer;
        dtProcessStart                  Date            := SYSDATE;
        strMessage01                    Varchar2(4000);
        strMessage02                    Varchar2(512);
        strMessage03                    Varchar2(512)   :='';
        strNewLine                      Varchar2(2)     := chr(13) || chr(10);
        strSqlCommand                   Varchar2(32767) :='';
        strSqlDynamic                   Varchar2(32767) :='';
        strClientInfo                   Varchar2(100);
        intRowCount                     Integer;
        intTotalRowCount                Integer         := 0;
        numSqlCode                      Number;
        strSqlErrm                      Varchar2(4000);
        intTries                        Integer;

BEGIN

strSqlCommand := 'DBMS_APPLICATION_INFO.SET_CLIENT_INFO';
DBMS_APPLICATION_INFO.SET_CLIENT_INFO (strProcessName);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_INIT';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_INIT
        (
                i_MartId                => strMartId,
                i_ProcessName           => strProcessName,
                i_ProcessStartTime      => dtProcessStart,
                o_ProcessSid            => intProcessSid
        );

/*  First Partition Group */
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'ALTER TABLE CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL TRUNCATE PARTITION UMBFAL2017';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
strSqlDynamic   := 'ALTER TABLE CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL TRUNCATE PARTITION UMDFAL2017';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
strSqlDynamic   := 'ALTER TABLE CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL TRUNCATE PARTITION UMLFAL2017';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_SF_TUITION_WAIVER_DTL');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL disable constraint PK_UM_F_SF_TUITION_WAIVER_DTL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_SF_TUITION_WAIVER_DTL
WITH WVR_CHRG_XWALK AS
(
SELECT DISTINCT
       CST.SETID,
       CST.STRM,
       WT.ITEM_TYPE AS WAVR_ITEM_TYPE,
       CST.ITEM_TYPE AS CHRG_ITEM_TYPE
    FROM CSSTG_OWNER.PS_CLASS_SBFEE_TBL CST,
         CSSTG_OWNER.PS_WAIVER_GRP_DTL WGD,
         CSSTG_OWNER.PS_WAIVER_TBL WT
    WHERE CST.SETID = WGD.SETID
    AND CST.WAIVER_GROUP = WGD.WAIVER_GROUP
    AND WGD.SETID = WT.SETID
    AND WGD.WAIVER_CODE = WT.WAIVER_CODE
    AND WT.EFFDT = (SELECT MAX(WTT.EFFDT)
                        FROM CSSTG_OWNER.PS_WAIVER_TBL WTT
                        WHERE WTT.SETID = WT.SETID
                        AND WTT.WAIVER_CODE = WT.WAIVER_CODE
                   )
    AND WGD.EFFDT = (SELECT MAX(WGDD.EFFDT)
                        FROM CSSTG_OWNER.PS_WAIVER_GRP_DTL WGDD
                        WHERE WGDD.SETID = WGD.SETID
                        AND WGDD.WAIVER_GROUP = WGD.WAIVER_GROUP
                        AND WGDD.WAIVER_CODE = WGD.WAIVER_CODE
                    )
UNION
SELECT DISTINCT
       A.SETID,
       A.STRM,
       C.ITEM_TYPE,
       A.ITEM_TYPE
    FROM CSSTG_OWNER.PS_TERM_FEE_TBL A,
         CSSTG_OWNER.PS_GRP_FEE_WAIVER B,
         CSSTG_OWNER.PS_WAIVER_TBL C
    WHERE A.SETID = B.BUSINESS_UNIT
    AND A.FEE_CODE = B.FEE_CODE
    AND B.BUSINESS_UNIT = C.SETID
    AND B.WAIVER_CODE = C.WAIVER_CODE
    AND B.EFFDT = (SELECT MAX(BB.EFFDT)
                        FROM CSSTG_OWNER.PS_GRP_FEE_WAIVER BB
                        WHERE BB.BUSINESS_UNIT = B.BUSINESS_UNIT
                        AND BB.SEL_GROUP = B.SEL_GROUP
                        AND BB.FEE_CODE = B.FEE_CODE
                        AND BB.WAIVER_CODE = B.WAIVER_CODE
                  )
    AND C.EFF_STATUS = 'A'
    AND C.EFFDT = (SELECT MAX(CC.EFFDT)
                        FROM CSSTG_OWNER.PS_WAIVER_TBL CC
                        WHERE CC.SETID = C.SETID
                        AND CC.WAIVER_CODE = C.WAIVER_CODE
                  )
UNION
/*UMDAR EXCEPTIONS - NOT IN PS_WAIVER_TBL*/
SELECT 'UMDAR', '2710', '600000060680', '200000020050' FROM DUAL
UNION
SELECT 'UMDAR', '2730', '600000060680', '200000020050' FROM DUAL
UNION
SELECT 'UMDAR', '2710', '600000060730', '200000020130' FROM DUAL
UNION
SELECT 'UMDAR', '2730', '600000060730', '200000020130' FROM DUAL
),
EQTN_VAR AS
(
SELECT DISTINCT C.BUSINESS_UNIT, A.EMPLID, C.ITEM_TERM,
MAX(VARIABLE_CHAR7) AS SPONSOR_ID,
SUM(CASE
        WHEN(A.INSTITUTION = 'UMBOS'
             AND
             (
              A.VARIABLE_CHAR1 = C.VARIABLE_CHAR1
              OR
              A.VARIABLE_CHAR3 = C.VARIABLE_CHAR3
             )
            )
        THEN(1)
        ELSE (0)
     END
    ) AS UMB_STDNT_CNT
FROM CSSTG_OWNER.PS_STDNT_EQUTN_VAR A, CSSTG_OWNER.UM_SF_WAIVER_CONFIG C
WHERE A.INSTITUTION = C.BUSINESS_UNIT
AND A.STRM = C.ITEM_TERM
AND A.STRM IN ('2710')
GROUP BY C.BUSINESS_UNIT, A.EMPLID, C.ITEM_TERM
),
ITEM_DTL AS
(
SELECT A.*,
       CASE
                WHEN(A.BUSINESS_UNIT = 'UMBOS' AND D.UMB_STDNT_CNT >= 1)
                THEN(1)
                ELSE(0)
       END AS UMB_STNDT_CNT,
       SUM(
           CASE
                WHEN(A.BUSINESS_UNIT = 'UMDAR' AND A.ITEM_TYPE = C.ITEM_TYPE)
                THEN(1)
                ELSE 0
           END
           ) OVER (PARTITION BY A.COMMON_ID) AS UMD_STNDT_CNT,
       SUM(
           CASE
                WHEN(A.BUSINESS_UNIT = 'UMDAR' AND A.ITEM_TYPE = C.ITEM_TYPE)
                THEN(1)
                ELSE 0
           END
           ) OVER (PARTITION BY A.COMMON_ID, A.ITEM_TYPE) AS UMD_ITEM_CNT,
       SUM(
           CASE
                WHEN(A.BUSINESS_UNIT = 'UMLOW' AND A.ITEM_TYPE = C.ITEM_TYPE)
                THEN(1)
                ELSE 0
           END
           ) OVER (PARTITION BY A.COMMON_ID) AS UML_STNDT_CNT,
       SUM(
           CASE
                WHEN(A.BUSINESS_UNIT = 'UMLOW' AND A.ITEM_TYPE = C.ITEM_TYPE)
                THEN(1)
                ELSE 0
           END
           ) OVER (PARTITION BY A.COMMON_ID, A.ITEM_TYPE) AS UML_ITEM_CNT,
       SUM(
           CASE
                WHEN(A.BUSINESS_UNIT = 'UMLOW' AND A.ITEM_TYPE = '600200000106')
                THEN(1)
                ELSE 0
           END
           ) OVER (PARTITION BY A.COMMON_ID) AS UML_600200000106_CNT,
       SUM(
           CASE
                WHEN(A.BUSINESS_UNIT = 'UMLOW' AND A.ITEM_TYPE = '600200000020')
                THEN(1)
                ELSE 0
           END
           ) OVER (PARTITION BY A.COMMON_ID) AS UML_600200000020_CNT,
       B.DESCR,
       D.SPONSOR_ID,
       CASE
            WHEN(D.SPONSOR_ID in ('-', ' ')) THEN('STATE/DHE')
            WHEN(D.SPONSOR_ID IS NULL) THEN('UNKNOWN')
            ELSE('UMASS')
       END AS BENEFICIARY_TYPE
    FROM CSSTG_OWNER.PS_ITEM_SF A,
         (SELECT ITM_TBL.SETID, ITM_TBL.ITEM_TYPE, ITM_TBL.DESCR,
                ROW_NUMBER() OVER (PARTITION BY SETID, ITEM_TYPE ORDER BY EFFDT DESC) AS ITEM_ORDER
            FROM CSSTG_OWNER.PS_ITEM_TYPE_TBL ITM_TBL
         ) B,
         CSSTG_OWNER.UM_SF_WAIVER_CONFIG C,
         EQTN_VAR D
    WHERE A.BUSINESS_UNIT = B.SETID
    AND A.ITEM_TYPE = B.ITEM_TYPE
    AND A.BUSINESS_UNIT = C.BUSINESS_UNIT(+)
    AND A.ITEM_TERM = C.ITEM_TERM(+)
    AND A.ITEM_TYPE = C.ITEM_TYPE(+)
    AND A.BUSINESS_UNIT = D.BUSINESS_UNIT(+)
    AND A.ITEM_TERM = D.ITEM_TERM(+)
    AND A.COMMON_ID = D.EMPLID(+)
    AND B.ITEM_ORDER = 1
    AND A.ITEM_TYPE_CD IN ('C', 'W')
    AND A.ITEM_AMT <> 0
    AND A.ITEM_TERM IN ('2710')
),
CHRG AS
(
SELECT DISTINCT T3.BUSINESS_UNIT, T3.ITEM_TERM, T3.COMMON_ID, T3.SPONSOR_ID, T3.BENEFICIARY_TYPE,
       T3.ACAD_CAREER, T3.ACAD_YEAR, T3.ITEM_TYPE, T3.DESCR,
       SUM(T3.ITEM_AMT) OVER (PARTITION BY T3.BUSINESS_UNIT, T3.ITEM_TERM, T3.COMMON_ID, T3.ITEM_TYPE, T3.ACAD_CAREER) AS ITEM_AMT
    FROM(SELECT DISTINCT T0.BUSINESS_UNIT, T0.ITEM_TERM, T0.COMMON_ID, T0.ACAD_CAREER, T0.ACAD_YEAR,
                T0.ITEM_TYPE, T0.DESCR, T0.ITEM_NBR, T0.ITEM_AMT, T0.SPONSOR_ID, T0.BENEFICIARY_TYPE
            FROM ITEM_DTL T0
         INNER JOIN WVR_CHRG_XWALK T1
         ON T0.BUSINESS_UNIT = T1.SETID
         AND T0.ITEM_TERM = T1.STRM
         AND T0.ITEM_TYPE = T1.CHRG_ITEM_TYPE
         AND T0.ITEM_TYPE_CD = 'C'
         AND
            (
             T0.BUSINESS_UNIT = 'UMBOS' AND T0.UMB_STNDT_CNT > 0
             OR
             T0.BUSINESS_UNIT = 'UMDAR' AND T0.UMD_STNDT_CNT > 0
             OR
             T0.BUSINESS_UNIT = 'UMLOW' AND (
                                             T0.UML_STNDT_CNT > 0
                                             OR
                                             (UML_600200000106_CNT >= 1 and UML_600200000020_CNT >= 1)
                                            )
            )
         --WHERE T0.COMMON_ID = '00997211'
        ) T3
),
WVR AS
(
SELECT DISTINCT T3.BUSINESS_UNIT, T3.ITEM_TERM, T3.COMMON_ID, T3.SPONSOR_ID, T3.BENEFICIARY_TYPE,
       T3.ACAD_CAREER, T3.ACAD_YEAR, T3.ITEM_TYPE, T3.DESCR,
       SUM(T3.ITEM_AMT) OVER (PARTITION BY T3.BUSINESS_UNIT, T3.ITEM_TERM, T3.COMMON_ID, T3.ITEM_TYPE, T3.ACAD_CAREER) AS ITEM_AMT
    FROM(SELECT DISTINCT T0.BUSINESS_UNIT, T0.ITEM_TERM, T0.COMMON_ID, T0.ACAD_CAREER, T0.ACAD_YEAR,
                T0.ITEM_TYPE, T0.DESCR, T0.ITEM_NBR, T0.ITEM_AMT, T0.SPONSOR_ID, T0.BENEFICIARY_TYPE
            FROM ITEM_DTL T0
         INNER JOIN WVR_CHRG_XWALK T1
         ON T0.BUSINESS_UNIT = T1.SETID
         AND T0.ITEM_TERM = T1.STRM
         AND T0.ITEM_TYPE = T1.WAVR_ITEM_TYPE
         AND T0.ITEM_TYPE_CD = 'W'
         AND
            (
             T0.BUSINESS_UNIT = 'UMBOS' AND T0.UMB_STNDT_CNT > 0
             OR
             (T0.BUSINESS_UNIT = 'UMDAR' AND UMD_STNDT_CNT > 0 AND UMD_ITEM_CNT > 0)
             OR
             (
              T0.BUSINESS_UNIT = 'UMLOW'
              AND(
                  UML_STNDT_CNT > 0
                  OR
                  (UML_600200000106_CNT >= 1 and UML_600200000020_CNT >= 1)
                 )
              AND(
                  UML_ITEM_CNT > 0
                  OR
                  T0.ITEM_TYPE = '600200000106' and UML_600200000020_CNT >= 1
                 )
             )
            )
         --WHERE T0.COMMON_ID = '00997211'
        ) T3
)
SELECT CHRG.BUSINESS_UNIT AS INSTITUTION_CD,
       CHRG.ITEM_TERM,
       'Fall 2017' AS ITEM_TERM_LD,
       CHRG.COMMON_ID AS PERSON_ID,
       CHRG.ITEM_TYPE,
       CHRG.DESCR AS ITEM_TYPE_DESCR,
       CHRG.ACAD_CAREER,
       -1 AS INSTITUTION_SID,
       -1 AS ITEM_TERM_SID,
       -1 AS ACAD_CAR_SID,
       -1 AS PERSON_SID,
       -1 AS ITEM_TYPE_SID,
       CHRG.ACAD_YEAR,
       CHRG.SPONSOR_ID,
       CHRG.BENEFICIARY_TYPE,
       CHRG.ITEM_AMT,
       CASE WHEN(CHRG.ITEM_TYPE LIKE '1%') THEN('TUITION') ELSE('FEES') END AS AMOUNT_TYPE,
       CASE WHEN(SUBSTR(CHRG.ITEM_TYPE,4,1) = '2') THEN ('CE') ELSE('DAY') END AS DAY_CE_FLAG,
       SUBSTR(CHRG.BUSINESS_UNIT, 1, 3)||'FAL2017' AS PARTITION_KEY,
       'N' AS LOAD_ERROR,
       'S' AS DATA_ORIGIN,
       SYSDATE AS CREATED_EW_DTTM,
       SYSDATE AS LASTUPD_EW_DTTM
    FROM CHRG
UNION
SELECT WVR.BUSINESS_UNIT AS INSTITUTION_CD,
       WVR.ITEM_TERM,
       'Fall 2017' AS ITEM_TERM_LD,
       WVR.COMMON_ID AS PERSON_ID,
       WVR.ITEM_TYPE,
       WVR.DESCR AS ITEM_TYPE_DESCR,
       WVR.ACAD_CAREER,
       -1 AS INSTITUTION_SID,
       -1 AS ITEM_TERM_SID,
       -1 AS ACAD_CAR_SID,
       -1 AS PERSON_SID,
       -1 AS ITEM_TYPE_SID,
       WVR.ACAD_YEAR,
       WVR.SPONSOR_ID,
       WVR.BENEFICIARY_TYPE,
       WVR.ITEM_AMT,
       'WAIVER' AS AMOUNT_TYPE,
       CASE WHEN(SUBSTR(WVR.ITEM_TYPE,1,4) = '6002') THEN ('CE') ELSE('DAY') END AS DAY_CE_FLAG,
       SUBSTR(WVR.BUSINESS_UNIT, 1, 3)||'FAL2017' AS PARTITION_KEY,
       'N' AS LOAD_ERROR,
       'S' AS DATA_ORIGIN,
       SYSDATE AS CREATED_EW_DTTM,
       SYSDATE AS LASTUPD_EW_DTTM
    FROM WVR
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

/*
strMessage01    := '# of UM_F_SF_TUITION_WAIVER_DTL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SF_TUITION_WAIVER_DTL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SF_TUITION_WAIVER_DTL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );
*/

/*  Second Partition Group */
strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'ALTER TABLE CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL TRUNCATE PARTITION UMBSPR2018';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
strSqlDynamic   := 'ALTER TABLE CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL TRUNCATE PARTITION UMDSPR2018';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
strSqlDynamic   := 'ALTER TABLE CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL TRUNCATE PARTITION UMLSPR2018';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL';
insert /*+ append enable_parallel_dml parallel(8) */ into UM_F_SF_TUITION_WAIVER_DTL
WITH WVR_CHRG_XWALK AS
(
SELECT DISTINCT
       CST.SETID,
       CST.STRM,
       WT.ITEM_TYPE AS WAVR_ITEM_TYPE,
       CST.ITEM_TYPE AS CHRG_ITEM_TYPE
    FROM CSSTG_OWNER.PS_CLASS_SBFEE_TBL CST,
         CSSTG_OWNER.PS_WAIVER_GRP_DTL WGD,
         CSSTG_OWNER.PS_WAIVER_TBL WT
    WHERE CST.SETID = WGD.SETID
    AND CST.WAIVER_GROUP = WGD.WAIVER_GROUP
    AND WGD.SETID = WT.SETID
    AND WGD.WAIVER_CODE = WT.WAIVER_CODE
    AND WT.EFFDT = (SELECT MAX(WTT.EFFDT)
                        FROM CSSTG_OWNER.PS_WAIVER_TBL WTT
                        WHERE WTT.SETID = WT.SETID
                        AND WTT.WAIVER_CODE = WT.WAIVER_CODE
                   )
    AND WGD.EFFDT = (SELECT MAX(WGDD.EFFDT)
                        FROM CSSTG_OWNER.PS_WAIVER_GRP_DTL WGDD
                        WHERE WGDD.SETID = WGD.SETID
                        AND WGDD.WAIVER_GROUP = WGD.WAIVER_GROUP
                        AND WGDD.WAIVER_CODE = WGD.WAIVER_CODE
                    )
UNION
SELECT DISTINCT
       A.SETID,
       A.STRM,
       C.ITEM_TYPE,
       A.ITEM_TYPE
    FROM CSSTG_OWNER.PS_TERM_FEE_TBL A,
         CSSTG_OWNER.PS_GRP_FEE_WAIVER B,
         CSSTG_OWNER.PS_WAIVER_TBL C
    WHERE A.SETID = B.BUSINESS_UNIT
    AND A.FEE_CODE = B.FEE_CODE
    AND B.BUSINESS_UNIT = C.SETID
    AND B.WAIVER_CODE = C.WAIVER_CODE
    AND B.EFFDT = (SELECT MAX(BB.EFFDT)
                        FROM CSSTG_OWNER.PS_GRP_FEE_WAIVER BB
                        WHERE BB.BUSINESS_UNIT = B.BUSINESS_UNIT
                        AND BB.SEL_GROUP = B.SEL_GROUP
                        AND BB.FEE_CODE = B.FEE_CODE
                        AND BB.WAIVER_CODE = B.WAIVER_CODE
                  )
    AND C.EFF_STATUS = 'A'
    AND C.EFFDT = (SELECT MAX(CC.EFFDT)
                        FROM CSSTG_OWNER.PS_WAIVER_TBL CC
                        WHERE CC.SETID = C.SETID
                        AND CC.WAIVER_CODE = C.WAIVER_CODE
                  )
UNION
/*UMDAR EXCEPTIONS - NOT IN PS_WAIVER_TBL*/
SELECT 'UMDAR', '2710', '600000060680', '200000020050' FROM DUAL
UNION
SELECT 'UMDAR', '2730', '600000060680', '200000020050' FROM DUAL
UNION
SELECT 'UMDAR', '2710', '600000060730', '200000020130' FROM DUAL
UNION
SELECT 'UMDAR', '2730', '600000060730', '200000020130' FROM DUAL
),
EQTN_VAR AS
(
SELECT DISTINCT C.BUSINESS_UNIT, A.EMPLID, C.ITEM_TERM,
MAX(VARIABLE_CHAR7) AS SPONSOR_ID,
SUM(CASE
        WHEN(A.INSTITUTION = 'UMBOS'
             AND
             (
              A.VARIABLE_CHAR1 = C.VARIABLE_CHAR1
              OR
              A.VARIABLE_CHAR3 = C.VARIABLE_CHAR3
             )
            )
        THEN(1)
        ELSE (0)
     END
    ) AS UMB_STDNT_CNT
FROM CSSTG_OWNER.PS_STDNT_EQUTN_VAR A, CSSTG_OWNER.UM_SF_WAIVER_CONFIG C
WHERE A.INSTITUTION = C.BUSINESS_UNIT
AND A.STRM = C.ITEM_TERM
AND A.STRM IN ('2730')
GROUP BY C.BUSINESS_UNIT, A.EMPLID, C.ITEM_TERM
),
ITEM_DTL AS
(
SELECT A.*,
       CASE
                WHEN(A.BUSINESS_UNIT = 'UMBOS' AND D.UMB_STDNT_CNT >= 1)
                THEN(1)
                ELSE(0)
       END AS UMB_STNDT_CNT,
       SUM(
           CASE
                WHEN(A.BUSINESS_UNIT = 'UMDAR' AND A.ITEM_TYPE = C.ITEM_TYPE)
                THEN(1)
                ELSE 0
           END
           ) OVER (PARTITION BY A.COMMON_ID) AS UMD_STNDT_CNT,
       SUM(
           CASE
                WHEN(A.BUSINESS_UNIT = 'UMDAR' AND A.ITEM_TYPE = C.ITEM_TYPE)
                THEN(1)
                ELSE 0
           END
           ) OVER (PARTITION BY A.COMMON_ID, A.ITEM_TYPE) AS UMD_ITEM_CNT,
       SUM(
           CASE
                WHEN(A.BUSINESS_UNIT = 'UMLOW' AND A.ITEM_TYPE = C.ITEM_TYPE)
                THEN(1)
                ELSE 0
           END
           ) OVER (PARTITION BY A.COMMON_ID) AS UML_STNDT_CNT,
       SUM(
           CASE
                WHEN(A.BUSINESS_UNIT = 'UMLOW' AND A.ITEM_TYPE = C.ITEM_TYPE)
                THEN(1)
                ELSE 0
           END
           ) OVER (PARTITION BY A.COMMON_ID, A.ITEM_TYPE) AS UML_ITEM_CNT,
       SUM(
           CASE
                WHEN(A.BUSINESS_UNIT = 'UMLOW' AND A.ITEM_TYPE = '600200000106')
                THEN(1)
                ELSE 0
           END
           ) OVER (PARTITION BY A.COMMON_ID) AS UML_600200000106_CNT,
       SUM(
           CASE
                WHEN(A.BUSINESS_UNIT = 'UMLOW' AND A.ITEM_TYPE = '600200000020')
                THEN(1)
                ELSE 0
           END
           ) OVER (PARTITION BY A.COMMON_ID) AS UML_600200000020_CNT,
       B.DESCR,
       D.SPONSOR_ID,
       CASE
            WHEN(D.SPONSOR_ID in ('-', ' ')) THEN('STATE/DHE')
            WHEN(D.SPONSOR_ID IS NULL) THEN('UNKNOWN')
            ELSE('UMASS')
       END AS BENEFICIARY_TYPE
    FROM CSSTG_OWNER.PS_ITEM_SF A,
         (SELECT ITM_TBL.SETID, ITM_TBL.ITEM_TYPE, ITM_TBL.DESCR,
                ROW_NUMBER() OVER (PARTITION BY SETID, ITEM_TYPE ORDER BY EFFDT DESC) AS ITEM_ORDER
            FROM CSSTG_OWNER.PS_ITEM_TYPE_TBL ITM_TBL
         ) B,
         CSSTG_OWNER.UM_SF_WAIVER_CONFIG C,
         EQTN_VAR D
    WHERE A.BUSINESS_UNIT = B.SETID
    AND A.ITEM_TYPE = B.ITEM_TYPE
    AND A.BUSINESS_UNIT = C.BUSINESS_UNIT(+)
    AND A.ITEM_TERM = C.ITEM_TERM(+)
    AND A.ITEM_TYPE = C.ITEM_TYPE(+)
    AND A.BUSINESS_UNIT = D.BUSINESS_UNIT(+)
    AND A.ITEM_TERM = D.ITEM_TERM(+)
    AND A.COMMON_ID = D.EMPLID(+)
    AND B.ITEM_ORDER = 1
    AND A.ITEM_TYPE_CD IN ('C', 'W')
    AND A.ITEM_AMT <> 0
    AND A.ITEM_TERM IN ('2730')
),
CHRG AS
(
SELECT DISTINCT T3.BUSINESS_UNIT, T3.ITEM_TERM, T3.COMMON_ID, T3.SPONSOR_ID, T3.BENEFICIARY_TYPE,
       T3.ACAD_CAREER, T3.ACAD_YEAR, T3.ITEM_TYPE, T3.DESCR,
       SUM(T3.ITEM_AMT) OVER (PARTITION BY T3.BUSINESS_UNIT, T3.ITEM_TERM, T3.COMMON_ID, T3.ITEM_TYPE, T3.ACAD_CAREER) AS ITEM_AMT
    FROM(SELECT DISTINCT T0.BUSINESS_UNIT, T0.ITEM_TERM, T0.COMMON_ID, T0.ACAD_CAREER, T0.ACAD_YEAR,
                T0.ITEM_TYPE, T0.DESCR, T0.ITEM_NBR, T0.ITEM_AMT, T0.SPONSOR_ID, T0.BENEFICIARY_TYPE
            FROM ITEM_DTL T0
         INNER JOIN WVR_CHRG_XWALK T1
         ON T0.BUSINESS_UNIT = T1.SETID
         AND T0.ITEM_TERM = T1.STRM
         AND T0.ITEM_TYPE = T1.CHRG_ITEM_TYPE
         AND T0.ITEM_TYPE_CD = 'C'
         AND
            (
             T0.BUSINESS_UNIT = 'UMBOS' AND T0.UMB_STNDT_CNT > 0
             OR
             T0.BUSINESS_UNIT = 'UMDAR' AND T0.UMD_STNDT_CNT > 0
             OR
             T0.BUSINESS_UNIT = 'UMLOW' AND (
                                             T0.UML_STNDT_CNT > 0
                                             OR
                                             (UML_600200000106_CNT >= 1 and UML_600200000020_CNT >= 1)
                                            )
            )
         --WHERE T0.COMMON_ID = '00997211'
        ) T3
),
WVR AS
(
SELECT DISTINCT T3.BUSINESS_UNIT, T3.ITEM_TERM, T3.COMMON_ID, T3.SPONSOR_ID, T3.BENEFICIARY_TYPE,
       T3.ACAD_CAREER, T3.ACAD_YEAR, T3.ITEM_TYPE, T3.DESCR,
       SUM(T3.ITEM_AMT) OVER (PARTITION BY T3.BUSINESS_UNIT, T3.ITEM_TERM, T3.COMMON_ID, T3.ITEM_TYPE, T3.ACAD_CAREER) AS ITEM_AMT
    FROM(SELECT DISTINCT T0.BUSINESS_UNIT, T0.ITEM_TERM, T0.COMMON_ID, T0.ACAD_CAREER, T0.ACAD_YEAR,
                T0.ITEM_TYPE, T0.DESCR, T0.ITEM_NBR, T0.ITEM_AMT, T0.SPONSOR_ID, T0.BENEFICIARY_TYPE
            FROM ITEM_DTL T0
         INNER JOIN WVR_CHRG_XWALK T1
         ON T0.BUSINESS_UNIT = T1.SETID
         AND T0.ITEM_TERM = T1.STRM
         AND T0.ITEM_TYPE = T1.WAVR_ITEM_TYPE
         AND T0.ITEM_TYPE_CD = 'W'
         AND
            (
             T0.BUSINESS_UNIT = 'UMBOS' AND T0.UMB_STNDT_CNT > 0
             OR
             (T0.BUSINESS_UNIT = 'UMDAR' AND UMD_STNDT_CNT > 0 AND UMD_ITEM_CNT > 0)
             OR
             (
              T0.BUSINESS_UNIT = 'UMLOW'
              AND(
                  UML_STNDT_CNT > 0
                  OR
                  (UML_600200000106_CNT >= 1 and UML_600200000020_CNT >= 1)
                 )
              AND(
                  UML_ITEM_CNT > 0
                  OR
                  T0.ITEM_TYPE = '600200000106' and UML_600200000020_CNT >= 1
                 )
             )
            )
         --WHERE T0.COMMON_ID = '00997211'
        ) T3
)
SELECT CHRG.BUSINESS_UNIT AS INSTITUTION_CD,
       CHRG.ITEM_TERM,
       'Spring 2018' AS ITEM_TERM_LD,
       CHRG.COMMON_ID AS PERSON_ID,
       CHRG.ITEM_TYPE,
       CHRG.DESCR AS ITEM_TYPE_DESCR,
       CHRG.ACAD_CAREER,
       -1 AS INSTITUTION_SID,
       -1 AS ITEM_TERM_SID,
       -1 AS ACAD_CAR_SID,
       -1 AS PERSON_SID,
       -1 AS ITEM_TYPE_SID,
       CHRG.ACAD_YEAR,
       CHRG.SPONSOR_ID,
       CHRG.BENEFICIARY_TYPE,
       CHRG.ITEM_AMT,
       CASE WHEN(CHRG.ITEM_TYPE LIKE '1%') THEN('TUITION') ELSE('FEES') END AS AMOUNT_TYPE,
       CASE WHEN(SUBSTR(CHRG.ITEM_TYPE,4,1) = '2') THEN ('CE') ELSE('DAY') END AS DAY_CE_FLAG,
       SUBSTR(CHRG.BUSINESS_UNIT, 1, 3)||'SPR2018' AS PARTITION_KEY,
       'N' AS LOAD_ERROR,
       'S' AS DATA_ORIGIN,
       SYSDATE AS CREATED_EW_DTTM,
       SYSDATE AS LASTUPD_EW_DTTM
    FROM CHRG
UNION
SELECT WVR.BUSINESS_UNIT AS INSTITUTION_CD,
       WVR.ITEM_TERM,
       'Spring 2018' AS ITEM_TERM_LD,
       WVR.COMMON_ID AS PERSON_ID,
       WVR.ITEM_TYPE,
       WVR.DESCR AS ITEM_TYPE_DESCR,
       WVR.ACAD_CAREER,
       -1 AS INSTITUTION_SID,
       -1 AS ITEM_TERM_SID,
       -1 AS ACAD_CAR_SID,
       -1 AS PERSON_SID,
       -1 AS ITEM_TYPE_SID,
       WVR.ACAD_YEAR,
       WVR.SPONSOR_ID,
       WVR.BENEFICIARY_TYPE,
       WVR.ITEM_AMT,
       'WAIVER' AS AMOUNT_TYPE,
       CASE WHEN(SUBSTR(WVR.ITEM_TYPE,1,4) = '6002') THEN ('CE') ELSE('DAY') END AS DAY_CE_FLAG,
       SUBSTR(WVR.BUSINESS_UNIT, 1, 3)||'SPR2018' AS PARTITION_KEY,
       'N' AS LOAD_ERROR,
       'S' AS DATA_ORIGIN,
       SYSDATE AS CREATED_EW_DTTM,
       SYSDATE AS LASTUPD_EW_DTTM
    FROM WVR
;

strSqlCommand   := 'SET intRowCount';
-- Not sure this will work
intRowCount     := intRowCount + SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_SF_TUITION_WAIVER_DTL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SF_TUITION_WAIVER_DTL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_SF_TUITION_WAIVER_DTL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_SF_TUITION_WAIVER_DTL enable constraint PK_UM_F_SF_TUITION_WAIVER_DTL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_SF_TUITION_WAIVER_DTL');

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_SUCCESS';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_SUCCESS;

strMessage01    := strProcessName || ' is complete.';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

EXCEPTION
    WHEN OTHERS THEN
        COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_EXCEPTION
                (
                        i_SqlCommand   => strSqlCommand,
                        i_SqlCode      => SQLCODE,
                        i_SqlErrm      => SQLERRM
                );

END UM_F_SF_TUITION_WAIVER_DTL_P;
/
