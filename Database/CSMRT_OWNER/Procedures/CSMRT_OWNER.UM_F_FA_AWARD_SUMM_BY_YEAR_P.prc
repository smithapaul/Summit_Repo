DROP PROCEDURE CSMRT_OWNER.UM_F_FA_AWARD_SUMM_BY_YEAR_P
/

--
-- UM_F_FA_AWARD_SUMM_BY_YEAR_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_F_FA_AWARD_SUMM_BY_YEAR_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--Loads table                -- UM_F_FA_AWARD_SUMM_BY_YEAR
--V01 12/13/2018             -- srikanth ,pabbu converted to proc from sql scripts

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_F_FA_AWARD_SUMM_BY_YEAR';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_F_FA_AWARD_SUMM_BY_YEAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_F_FA_AWARD_SUMM_BY_YEAR';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_F_FA_AWARD_SUMM_BY_YEAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_F_FA_AWARD_SUMM_BY_YEAR');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_AWARD_SUMM_BY_YEAR disable constraint PK_UM_F_FA_AWARD_SUMM_BY_YEAR';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_F_FA_AWARD_SUMM_BY_YEAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_F_FA_AWARD_SUMM_BY_YEAR';
INSERT /*+ append enable_parallel_dml parallel(8) */ INTO CSMRT_OWNER.UM_F_FA_AWARD_SUMM_BY_YEAR
WITH T0_AID AS
(
SELECT /*+ inline */
     T571889.INSTITUTION_CD,
     T571889.INSTITUTION_SID,
     T572353.PERSON_ID,
     T572353.PERSON_SID,
     T998546.AID_YEAR,
     T1051154.ACAD_CAR_CD,
     T1051154.ACAD_CAR_SID,
     CASE
        WHEN(T998546.ISIR_CNT = 0)
        THEN(0)
        ELSE(T1008770.FED_NEED)
     END AS FED_NEED,
     SUM(
         CASE
            WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
            WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
            ELSE(T1002438.OFFER_BALANCE)
         END
        ) AS AWARD_AMOUNT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_1 = 'GRANT')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS GRANT_AMT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_1 = 'WORK')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS WORK_AMT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_1 = 'LOAN')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS LOAN_AMT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_2 = 'G-FED')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS G_FED_AMT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_2 = 'G-STATE')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS G_STATE_AMT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_2 = 'G-INSTIT')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS G_INSTIT_AMT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_2 = 'O-WAIV')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS O_WAIV_AMT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_2 = 'O-ATHLT')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS O_ATHLT_AMT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_2 = 'G-PRVTE')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS G_PRVTE_AMT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_2 = 'SH-CWP')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS SH_CWP_AMT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_2 = 'SH-FWS')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS SH_FWS_AMT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_2 = 'SH-SLOAN')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS SH_SLOAN_AMT,
     SUM(
        CASE
            WHEN(T1075637.CATEGORY_2 = 'O-PLOAN')
            THEN(
                 CASE
                    WHEN(T1075637.AWARD_AMT_TYPE = 'D') THEN(T1002438.DISBURSED_BALANCE)
                    WHEN(T1075637.AWARD_AMT_TYPE = 'A') THEN(T1002438.ACCEPT_BALANCE)
                    ELSE(T1002438.OFFER_BALANCE)
                 END
                )
            ELSE(0)
        END
        ) AS O_PLOAN_AMT
FROM
     CSMRT_OWNER.UM_D_ACAD_CAR_VW T1051154 /* FA Award - D_ACAD_CAR */ ,
     CSMRT_OWNER.UM_D_FA_ITEM_TYPE_CTGRY_VW T1075637 /* FA - D_FA_ITEM_TYPE_CTGRY */ ,
     CSMRT_OWNER.UM_D_FA_ITEM_TYPE_VW T1006851 /* FA - D_FA_ITEM_TYPE */ ,
     CSMRT_OWNER.UM_D_INSTITUTION_VW T571889 /* D_INSTITUTION */ ,
--     CSMRT_OWNER.UM_D_PERSON_CS_VW T572353 /* D_PERSON */ ,
     CSMRT_OWNER.PS_D_PERSON T572353 /* D_PERSON */ ,
     CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR_VW T998546 /* F_FA_STDNT_AID_ISIR */
     LEFT OUTER JOIN CSMRT_OWNER.UM_F_FA_STDNT_AWRD_PERIOD_VW T1008770 /* F_FA_STDNT_AWRD_PERIOD */
       ON T998546.AID_YEAR = T1008770.AID_YEAR
      AND T998546.INSTITUTION_CD = T1008770.INSTITUTION_CD
      AND T998546.PERSON_ID = T1008770.PERSON_ID
      AND T998546.SRC_SYS_ID = T1008770.SRC_SYS_ID,
     CSMRT_OWNER.UM_F_FA_AWARD_DISB_VW T1002438 /* F_FA_AWARD_DISB */ ,
     CSMRT_OWNER.UM_F_FA_STDNT_AWARDS_VW T1020465 /* F_FA_STDNT_AWARDS */
WHERE T571889.INSTITUTION_SID = T998546.INSTITUTION_SID
  AND T572353.PERSON_SID = T998546.PERSON_SID
  AND T998546.AID_YEAR = T1020465.AID_YEAR
  AND T998546.AID_YEAR IN (
                           SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL
                           UNION
                           SELECT TO_CHAR((TO_CHAR(SYSDATE, 'YYYY') - 1)) FROM DUAL
                           UNION
                           SELECT TO_CHAR((TO_CHAR(SYSDATE, 'YYYY') - 2)) FROM DUAL
                           )
  AND T998546.INSTITUTION_CD = T1020465.INSTITUTION_CD
  AND T998546.PERSON_ID = T1020465.PERSON_ID
  AND T998546.SRC_SYS_ID = T1020465.SRC_SYS_ID
  AND T1002438.ACAD_CAR_CD = T1020465.ACAD_CAR_CD
  AND T1002438.AID_YEAR = T998546.AID_YEAR
  AND T1002438.INSTITUTION_CD = T1020465.INSTITUTION_CD
  AND T1002438.ITEM_TYPE = T1020465.ITEM_TYPE
  AND T1002438.PERSON_ID = T1020465.PERSON_ID
  AND T1002438.SRC_SYS_ID = T1020465.SRC_SYS_ID
  AND T1006851.AID_YEAR = T1075637.AID_YEAR
  AND T1006851.INSTITUTION_CD = T1075637.INSTITUTION_CD
  AND T1006851.ITEM_TYPE = T1075637.ITEM_TYPE
  AND T1006851.ITEM_TYPE_SID = T1020465.ITEM_TYPE_SID
  AND T1006851.SRC_SYS_ID = T1075637.SRC_SYS_ID
  AND T1020465.ACAD_CAR_SID = T1051154.ACAD_CAR_SID
--AND T571889.INSTITUTION_CD IN ('-', 'UMBOS') AND T1051154.INSTITUTION_CD IN ('-', 'UMBOS') AND T1075637.INSTITUTION_CD IN ('-', 'UMBOS')
  AND T1002438.OFFER_BALANCE > 0 AND T1075637.REPORT_NAME = 'PRES_CDSH1_H2'
  AND T1008770.AWARD_PERIOD = 'A' --AND T1075637.USE_FA_SF_BRIDGE = 'N'
  AND
    (
     T1002438.TERM_CD like ('%10')
     OR
     T1002438.TERM_CD like ('%30')
    )
group by T571889.INSTITUTION_CD,
         T571889.INSTITUTION_SID,
         T572353.PERSON_ID,
         T572353.PERSON_SID,
         T998546.AID_YEAR,
         T1051154.ACAD_CAR_CD, T1051154.ACAD_CAR_SID,
         CASE WHEN(T998546.ISIR_CNT = 0)
              THEN(0)
              ELSE(T1008770.FED_NEED)
          END
/*
ORDER BY T571889.INSTITUTION_CD,
     T572353.PERSON_ID,
     T998546.AID_YEAR,
     T1051154.ACAD_CAR_CD
*/
),
ALL_AWARDS AS
(
SELECT /*+ inline */
     INSTITUTION_CD,
     INSTITUTION_SID,
     PERSON_ID,
     PERSON_SID,
     AID_YEAR,
     ACAD_CAR_CD,
     ACAD_CAR_SID,
     SUM(FED_NEED) AS FED_NEED,
     SUM(AWARD_AMOUNT) AS AWARD_AMOUNT,
     SUM(GRANT_AMT)AS GRANT_AMT,
     SUM(WORK_AMT) AS WORK_AMT,
     SUM(LOAN_AMT) AS LOAN_AMT,
     SUM(G_FED_AMT) AS G_FED_AMT,
     SUM(G_STATE_AMT) AS G_STATE_AMT,
     SUM(G_INSTIT_AMT) AS G_INSTIT_AMT,
     SUM(O_WAIV_AMT) AS O_WAIV_AMT,
     SUM(O_ATHLT_AMT) AS O_ATHLT_AMT,
     SUM(G_PRVTE_AMT) AS G_PRVTE_AMT,
     SUM(SH_CWP_AMT) AS SH_CWP_AMT,
     SUM(SH_FWS_AMT) AS SH_FWS_AMT,
     SUM(SH_SLOAN_AMT) AS SH_SLOAN_AMT,
     SUM(O_PLOAN_AMT) AS O_PLOAN_AMT
FROM T0_AID
WHERE ROWNUM < 100000000    -- Added
GROUP BY INSTITUTION_CD, INSTITUTION_SID, PERSON_ID, PERSON_SID, AID_YEAR, ACAD_CAR_CD, ACAD_CAR_SID
),
T1_DECLINES AS
(
SELECT /*+ inline */ DISTINCT T998546.AID_YEAR,
     T1008976.ACTION_DTTM,
     T1008976.ACTION_DT,
     T1008976.AWARD_DISB_ACTION,
     T1008976.AWARD_DISB_ACTION_LD,
     T1051154.ACAD_CAR_CD,
     T1020465.AWARD_STATUS,
     T571889.INSTITUTION_CD,
     T1075637.CATEGORY_1,
     T1075637.CATEGORY_2,
     T1075637.REPORT_NAME,
     T572353.PERSON_ID,
     T1008976.OFFER_AMOUNT,
     T1006851.ITEM_TYPE,
     MAX(T1008976.ACTION_DTTM) OVER (PARTITION BY T571889.INSTITUTION_CD, T572353.PERSON_ID, T998546.AID_YEAR, T1006851.ITEM_TYPE, T1051154.ACAD_CAR_CD) MAX_ACTION_DTTM
FROM
     CSMRT_OWNER.UM_D_ACAD_CAR_VW T1051154 /* FA Award - D_ACAD_CAR */ ,
     CSMRT_OWNER.UM_D_FA_ITEM_TYPE_CTGRY_VW T1075637 /* FA - D_FA_ITEM_TYPE_CTGRY */ ,
     CSMRT_OWNER.UM_D_FA_ITEM_TYPE_VW T1006851 /* FA - D_FA_ITEM_TYPE */ ,
     CSMRT_OWNER.UM_D_INSTITUTION_VW T571889 /* D_INSTITUTION */ ,
--     CSMRT_OWNER.UM_D_PERSON_CS_VW T572353 /* D_PERSON */ ,
     CSMRT_OWNER.PS_D_PERSON T572353 /* D_PERSON */ ,
     CSMRT_OWNER.UM_F_FA_STDNT_AID_ISIR_VW T998546 /* F_FA_STDNT_AID_ISIR */ ,
     CSMRT_OWNER.UM_F_FA_STDNT_AWARDS_VW T1020465 /* F_FA_STDNT_AWARDS */
     LEFT OUTER JOIN CSMRT_OWNER.UM_F_FA_STDNT_AWRD_ACTV_VW T1008976 /* F_FA_STDNT_AWRD_ACTV */
       ON T1008976.ACAD_CAR_CD = T1020465.ACAD_CAR_CD
      AND T1008976.AID_YEAR = T1020465.AID_YEAR
      AND T1008976.INSTITUTION_CD = T1020465.INSTITUTION_CD
      AND T1008976.ITEM_TYPE = T1020465.ITEM_TYPE
      AND T1008976.PERSON_ID = T1020465.PERSON_ID
      AND T1008976.SRC_SYS_ID = T1020465.SRC_SYS_ID
WHERE T1006851.ITEM_TYPE_SID = T1020465.ITEM_TYPE_SID
  AND T571889.INSTITUTION_SID = T998546.INSTITUTION_SID
  AND T572353.PERSON_SID = T998546.PERSON_SID
  AND T998546.AID_YEAR = T1020465.AID_YEAR
  AND T998546.INSTITUTION_CD = T1020465.INSTITUTION_CD
  AND T998546.PERSON_ID = T1020465.PERSON_ID
  AND T998546.SRC_SYS_ID = T1020465.SRC_SYS_ID
  AND T998546.AID_YEAR IN (
                           SELECT TO_CHAR(SYSDATE, 'YYYY') FROM DUAL
                           UNION
                           SELECT TO_CHAR((TO_CHAR(SYSDATE, 'YYYY') - 1)) FROM DUAL
                          )
  AND T1020465.ACAD_CAR_SID = T1051154.ACAD_CAR_SID
  AND T1006851.AID_YEAR = T1075637.AID_YEAR
  AND T1006851.INSTITUTION_CD = T1075637.INSTITUTION_CD
  AND T1006851.ITEM_TYPE = T1075637.ITEM_TYPE
  AND T1006851.SRC_SYS_ID = T1075637.SRC_SYS_ID
  AND T1020465.AWARD_STATUS = 'D'
  --AND T571889.INSTITUTION_CD IN ('-', 'UMBOS') AND T1051154.INSTITUTION_CD IN ('-', 'UMBOS') AND T1075637.INSTITUTION_CD IN ('-', 'UMBOS')
  AND T1008976.AWARD_DISB_ACTION = 'B'
  AND T1008976.ACTION_DT <= SYSDATE
  AND T1075637.REPORT_NAME = 'PRES_CDSH1_H2'
  AND T1075637.USE_FA_SF_BRIDGE = 'N'
),
T2_DECLINES_SUMM AS
(
SELECT /*+ inline */
     INSTITUTION_CD,
     PERSON_ID,
     AID_YEAR,
     ACAD_CAR_CD,
     SUM(OFFER_AMOUNT) TOTAL_DECL,
     SUM(
        CASE
            WHEN(CATEGORY_1 = 'GRANT')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS GRANT_DECLINE_AMT,
     SUM(
        CASE
            WHEN(CATEGORY_1 = 'WORK')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS WORK_DECLINE_AMT,
     SUM(
        CASE
            WHEN(CATEGORY_1 = 'LOAN')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS LOAN_DECLINE_AMT,
     SUM(
        CASE
            WHEN(CATEGORY_2 = 'G-FED')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS G_FED_DECLINE_AMT,
     SUM(
        CASE
            WHEN(CATEGORY_2 = 'G-STATE')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS G_STATE_DECLINE_AMT,
     SUM(
        CASE
            WHEN(CATEGORY_2 = 'G-INSTIT')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS G_INSTIT_DECLINE_AMT,
     SUM(
        CASE
            WHEN(CATEGORY_2 = 'O-WAIV')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS O_WAIV_DECLINE_AMT,
     SUM(
        CASE
            WHEN(CATEGORY_2 = 'O-ATHLT')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS O_ATHLT_DECLINE_AMT,
     SUM(
        CASE
            WHEN(CATEGORY_2 = 'G-PRVTE')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS G_PRVTE_DECLINE_AMT,
     SUM(
        CASE
            WHEN(CATEGORY_2 = 'SH-CWP')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS SH_CWP_DECLINE_AMT,
     SUM(
        CASE
            WHEN(CATEGORY_2 = 'SH-FWS')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS SH_FWS_DECLINE_AMT,
     SUM(
        CASE
            WHEN(CATEGORY_2 = 'SH-SLOAN')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS SH_SLOAN_DECLINE_AMT,
     SUM(
        CASE
            WHEN(CATEGORY_2 = 'O-PLOAN')
            THEN(OFFER_AMOUNT)
            ELSE(0)
        END
        ) AS O_PLOAN_DECLINE_AMT
 FROM T1_DECLINES
WHERE ACTION_DTTM = MAX_ACTION_DTTM
--AND PERSON_ID = '00828012'
AND ROWNUM < 100000000      -- Added
GROUP BY INSTITUTION_CD, PERSON_ID, AID_YEAR, ACAD_CAR_CD
),
T4 AS
(
SELECT /*+ inline */
     ALL_AWARDS.INSTITUTION_CD,
     ALL_AWARDS.INSTITUTION_SID,
     ALL_AWARDS.PERSON_ID,
     ALL_AWARDS.PERSON_SID,
     ALL_AWARDS.AID_YEAR,
     ALL_AWARDS.ACAD_CAR_CD,
     ALL_AWARDS.ACAD_CAR_SID,
     ALL_AWARDS.FED_NEED,
     ALL_AWARDS.AWARD_AMOUNT,
     ALL_AWARDS.GRANT_AMT AS GRANT_AID,
     NVL(T2_DECLINES_SUMM.GRANT_DECLINE_AMT, 0) AS GRANT_AID_DECLINED,
     CASE
        WHEN(GRANT_AMT <= FED_NEED)
        THEN(GRANT_AMT)
        ELSE(FED_NEED)
     END AS GRANT_AID_NEED,
     CASE
        WHEN(GRANT_AMT > FED_NEED)
        THEN(GRANT_AMT - FED_NEED)
        ELSE(0)
     END AS GRANT_AID_NO_NEED,
     ALL_AWARDS.WORK_AMT AS WORK_AID,
     NVL(T2_DECLINES_SUMM.WORK_DECLINE_AMT, 0) AS WORK_AID_DECLINED,
     CASE
        WHEN(WORK_AMT <= (FED_NEED - GRANT_AMT))
        THEN(WORK_AMT)
        ELSE(CASE
                 WHEN((FED_NEED - GRANT_AMT)) <= 0
                 THEN(0)
                 ELSE(FED_NEED - GRANT_AMT)
             END
            )
     END AS WORK_AID_NEED,
     CASE
        WHEN(WORK_AMT <= (FED_NEED - GRANT_AMT))
        THEN(0)
        WHEN((FED_NEED - GRANT_AMT) <= 0)
        THEN(WORK_AMT)
        ELSE(WORK_AMT - (FED_NEED - GRANT_AMT))
     END AS WORK_AID_NO_NEED,
     CASE
        WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
        THEN(ALL_AWARDS.LOAN_AMT + NVL(T2_DECLINES_SUMM.LOAN_DECLINE_AMT, 0))
        ELSE(ALL_AWARDS.LOAN_AMT)
     END AS LOAN_AID,
     NVL(T2_DECLINES_SUMM.LOAN_DECLINE_AMT, 0) AS LOAN_AID_DECLINED,
     CASE
        WHEN(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(ALL_AWARDS.LOAN_AMT + NVL(T2_DECLINES_SUMM.LOAN_DECLINE_AMT, 0))
                ELSE(ALL_AWARDS.LOAN_AMT)
             END <= (FED_NEED - GRANT_AMT - WORK_AMT)
            )
        THEN(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(ALL_AWARDS.LOAN_AMT + NVL(T2_DECLINES_SUMM.LOAN_DECLINE_AMT, 0))
                ELSE(ALL_AWARDS.LOAN_AMT)
             END
            )
        ELSE(CASE
                 WHEN((FED_NEED - GRANT_AMT - WORK_AMT)) <= 0
                 THEN(0)
                 ELSE(FED_NEED - GRANT_AMT - WORK_AMT)
             END
            )
     END AS LOAN_AID_NEED,
     CASE
        WHEN(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(ALL_AWARDS.LOAN_AMT + NVL(T2_DECLINES_SUMM.LOAN_DECLINE_AMT, 0))
                ELSE(ALL_AWARDS.LOAN_AMT)
             END <= (FED_NEED - GRANT_AMT - WORK_AMT)
            )
        THEN(0)
        WHEN((FED_NEED - GRANT_AMT - WORK_AMT) <= 0)
        THEN(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(ALL_AWARDS.LOAN_AMT + NVL(T2_DECLINES_SUMM.LOAN_DECLINE_AMT, 0))
                ELSE(ALL_AWARDS.LOAN_AMT)
             END
             )
        ELSE(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(ALL_AWARDS.LOAN_AMT + NVL(T2_DECLINES_SUMM.LOAN_DECLINE_AMT, 0))
                ELSE(ALL_AWARDS.LOAN_AMT)
             END - (FED_NEED - GRANT_AMT - WORK_AMT)
            )
     END AS LOAN_AID_NO_NEED,
     ALL_AWARDS.G_FED_AMT AS G_FED_AID,
     NVL(T2_DECLINES_SUMM.G_FED_DECLINE_AMT, 0) AS G_FED_AID_DECLINED,
     CASE
        WHEN(G_FED_AMT <= FED_NEED)
        THEN(G_FED_AMT)
        ELSE(FED_NEED)
     END AS G_FED_AID_NEED,
     CASE
        WHEN(G_FED_AMT > FED_NEED)
        THEN(G_FED_AMT - FED_NEED)
        ELSE(0)
     END AS G_FED_AID_NO_NEED,
     ALL_AWARDS.G_STATE_AMT AS G_STATE_AID,
     NVL(T2_DECLINES_SUMM.G_STATE_DECLINE_AMT, 0) AS G_STATE_AID_DECLINED,
     CASE
        WHEN(G_STATE_AMT <= (FED_NEED - G_FED_AMT))
        THEN(G_STATE_AMT)
        ELSE(CASE
                 WHEN((FED_NEED - G_FED_AMT)) <= 0
                 THEN(0)
                 ELSE(FED_NEED - G_FED_AMT)
             END
            )
     END AS G_STATE_AID_NEED,
     CASE
        WHEN(G_STATE_AMT <= (FED_NEED - G_FED_AMT))
        THEN(0)
        WHEN((FED_NEED - G_FED_AMT) <= 0)
        THEN(G_STATE_AMT)
        ELSE(G_STATE_AMT - (FED_NEED - G_FED_AMT))
     END AS G_STATE_AID_NO_NEED,
     ALL_AWARDS.G_INSTIT_AMT AS G_INSTIT_AID,
     NVL(T2_DECLINES_SUMM.G_INSTIT_DECLINE_AMT, 0) AS G_INSTIT_AID_DECLINED,
     CASE
        WHEN(G_INSTIT_AMT <= (FED_NEED - G_FED_AMT - G_STATE_AMT))
        THEN(G_INSTIT_AMT)
        ELSE(CASE
                 WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT)) <= 0
                 THEN(0)
                 ELSE(FED_NEED - G_FED_AMT - G_STATE_AMT)
             END
            )
     END AS G_INSTIT_AID_NEED,
     CASE
        WHEN(G_INSTIT_AMT <= (FED_NEED - G_FED_AMT - G_STATE_AMT))
        THEN(0)
        WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT) <= 0)
        THEN(G_INSTIT_AMT)
        ELSE(G_INSTIT_AMT - (FED_NEED - G_FED_AMT - G_STATE_AMT))
     END AS G_INSTIT_AID_NO_NEED,
     ALL_AWARDS.O_WAIV_AMT AS O_WAIV_AID,
     NVL(T2_DECLINES_SUMM.O_WAIV_DECLINE_AMT, 0) AS O_WAIV_AID_DECLINED,
     CASE
        WHEN(O_WAIV_AMT <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT))
        THEN(O_WAIV_AMT)
        ELSE(CASE
                 WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT)) <= 0
                 THEN(0)
                 ELSE(FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT)
             END
            )
     END AS O_WAIV_AID_NEED,
     CASE
        WHEN(O_WAIV_AMT <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT))
        THEN(0)
        WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT) <= 0)
        THEN(O_WAIV_AMT)
        ELSE(O_WAIV_AMT - (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT))
     END AS O_WAIV_AID_NO_NEED,
     ALL_AWARDS.O_ATHLT_AMT AS ATHL_AID,
     NVL(T2_DECLINES_SUMM.O_ATHLT_DECLINE_AMT, 0) AS ATHL_AID_DECLINED,
     CASE
        WHEN(O_ATHLT_AMT <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT))
        THEN(O_ATHLT_AMT)
        ELSE(CASE
                 WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT)) <= 0
                 THEN(0)
                 ELSE(FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT)
             END
            )
     END AS ATHL_AID_NEED,
     CASE
        WHEN(O_ATHLT_AMT <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT))
        THEN(0)
        WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT) <= 0)
        THEN(O_ATHLT_AMT)
        ELSE(O_ATHLT_AMT - (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT))
     END AS ATHL_AID_NO_NEED,
     ALL_AWARDS.G_PRVTE_AMT AS G_PRVTE_AID,
     NVL(T2_DECLINES_SUMM.G_PRVTE_DECLINE_AMT, 0) AS G_PRVTE_AID_DECLINED,
     CASE
        WHEN(G_PRVTE_AMT <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT))
        THEN(G_PRVTE_AMT)
        ELSE(CASE
                 WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT)) <= 0
                 THEN(0)
                 ELSE(FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT)
             END
            )
     END AS G_PRVTE_AID_NEED,
     CASE
        WHEN(G_PRVTE_AMT <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT))
        THEN(0)
        WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT) <= 0)
        THEN(G_PRVTE_AMT)
        ELSE(G_PRVTE_AMT - (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT))
     END AS G_PRVTE_AID_NO_NEED,
     ALL_AWARDS.SH_CWP_AMT AS SH_CWP_AID,
     NVL(T2_DECLINES_SUMM.SH_CWP_DECLINE_AMT, 0) AS SH_CWP_AID_DECLINED,
     CASE
        WHEN(SH_CWP_AMT <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT))
        THEN(SH_CWP_AMT)
        ELSE(CASE
                 WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT)) <= 0
                 THEN(0)
                 ELSE(FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT)
             END
            )
     END AS SH_CWP_AID_NEED,
     CASE
        WHEN(SH_CWP_AMT <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT))
        THEN(0)
        WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT) <= 0)
        THEN(SH_CWP_AMT)
        ELSE(SH_CWP_AMT - (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT))
     END AS SH_CWP_AID_NO_NEED,
     ALL_AWARDS.SH_FWS_AMT AS SH_FWS_AID,
     NVL(T2_DECLINES_SUMM.SH_FWS_DECLINE_AMT, 0) AS SH_FWS_AID_DECLINED,
     CASE
        WHEN(SH_FWS_AMT <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT))
        THEN(SH_FWS_AMT)
        ELSE(CASE
                 WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT)) <= 0
                 THEN(0)
                 ELSE(FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT)
             END
            )
     END AS SH_FWS_AID_NEED,
     CASE
        WHEN(SH_FWS_AMT <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT))
        THEN(0)
        WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT) <= 0)
        THEN(SH_FWS_AMT)
        ELSE(SH_FWS_AMT - (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT))
     END AS SH_FWS_AID_NO_NEED,
     CASE
        WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
        THEN(SH_SLOAN_AMT + NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0))
        ELSE(SH_SLOAN_AMT)
     END AS SH_LOAN_AID,
     NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0) AS SH_LOAN_AID_DECLINED,
     CASE
        WHEN(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(SH_SLOAN_AMT + NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0))
                ELSE(SH_SLOAN_AMT)
             END <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT - SH_FWS_AMT))
        THEN(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(SH_SLOAN_AMT + NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0))
                ELSE(SH_SLOAN_AMT)
             END
            )
        ELSE(CASE
                 WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT - SH_FWS_AMT)) <= 0
                 THEN(0)
                 ELSE(FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT - SH_FWS_AMT)
             END
            )
     END AS SH_SLOAN_AID_NEED,
     CASE
        WHEN(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(SH_SLOAN_AMT + NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0))
                ELSE(SH_SLOAN_AMT)
             END <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT - SH_FWS_AMT))
        THEN(0)
        WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT - SH_FWS_AMT) <= 0)
        THEN(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(SH_SLOAN_AMT + NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0))
                ELSE(SH_SLOAN_AMT)
             END
            )
        ELSE(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(SH_SLOAN_AMT + NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0))
                ELSE(SH_SLOAN_AMT)
             END - (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT - SH_FWS_AMT))
     END AS SH_SLOAN_AID_NO_NEED,
     CASE
        WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
        THEN(O_PLOAN_AMT + NVL(T2_DECLINES_SUMM.O_PLOAN_DECLINE_AMT, 0))
        ELSE(O_PLOAN_AMT)
     END AS O_PLOAN_AID,
     NVL(T2_DECLINES_SUMM.O_PLOAN_DECLINE_AMT, 0) AS O_PLOAN_AID_DECLINED,
     CASE
        WHEN(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(O_PLOAN_AMT + NVL(T2_DECLINES_SUMM.O_PLOAN_DECLINE_AMT, 0))
                ELSE(O_PLOAN_AMT)
             END <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT - SH_FWS_AMT
                     - CASE
                           WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                           THEN(SH_SLOAN_AMT + NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0))
                           ELSE(SH_SLOAN_AMT)
                       END
                    )
            )
        THEN(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(O_PLOAN_AMT + NVL(T2_DECLINES_SUMM.O_PLOAN_DECLINE_AMT, 0))
                ELSE(O_PLOAN_AMT)
             END
            )
        ELSE(CASE
                 WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT - SH_FWS_AMT - CASE
                                                                                                                                                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                                                                                                                                                THEN(SH_SLOAN_AMT + NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0))
                                                                                                                                                ELSE(SH_SLOAN_AMT)
                                                                                                                                              END)
                     ) <= 0
                 THEN(0)
                 ELSE(FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT - SH_FWS_AMT
                      - CASE
                            WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                            THEN(SH_SLOAN_AMT + NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0))
                            ELSE(SH_SLOAN_AMT)
                        END
                     )
             END
            )
     END AS O_PLOAN_AID_NEED,
     CASE
        WHEN(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(O_PLOAN_AMT + NVL(T2_DECLINES_SUMM.O_PLOAN_DECLINE_AMT, 0))
                ELSE(O_PLOAN_AMT)
             END <= (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT - SH_FWS_AMT
                     - CASE
                          WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                          THEN(SH_SLOAN_AMT + NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0))
                          ELSE(SH_SLOAN_AMT)
                       END
                    )
            )
        THEN(0)
        WHEN((FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT - SH_FWS_AMT
              - CASE
                   WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                   THEN(SH_SLOAN_AMT + NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0))
                   ELSE(SH_SLOAN_AMT)
                END
             ) <= 0
            )
        THEN(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(O_PLOAN_AMT + NVL(T2_DECLINES_SUMM.O_PLOAN_DECLINE_AMT, 0))
                ELSE(O_PLOAN_AMT)
             END
            )
        ELSE(CASE
                WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                THEN(O_PLOAN_AMT + NVL(T2_DECLINES_SUMM.O_PLOAN_DECLINE_AMT, 0))
                ELSE(O_PLOAN_AMT)
             END - (FED_NEED - G_FED_AMT - G_STATE_AMT - G_INSTIT_AMT - O_WAIV_AMT - O_ATHLT_AMT - G_PRVTE_AMT - SH_CWP_AMT - SH_FWS_AMT
                    - CASE
                         WHEN(ALL_AWARDS.INSTITUTION_CD IN ('UMBOS'))
                         THEN(SH_SLOAN_AMT + NVL(T2_DECLINES_SUMM.SH_SLOAN_DECLINE_AMT, 0))
                         ELSE(SH_SLOAN_AMT)
                      END
                   )
            )
     END AS O_PLOAN_AID_NO_NEED
FROM ALL_AWARDS
LEFT OUTER JOIN T2_DECLINES_SUMM
  ON ALL_AWARDS.INSTITUTION_CD = T2_DECLINES_SUMM.INSTITUTION_CD
 AND ALL_AWARDS.PERSON_ID = T2_DECLINES_SUMM.PERSON_ID
 AND ALL_AWARDS.AID_YEAR = T2_DECLINES_SUMM.AID_YEAR
 AND ALL_AWARDS.ACAD_CAR_CD = T2_DECLINES_SUMM.ACAD_CAR_CD
--WHERE ALL_AWARDS.PERSON_ID =
--'00063615' --UML
--'00015682' --UML
--'01617449' --UML
--'01469112' --UMB
--ORDER BY ALL_AWARDS.PERSON_ID
)
SELECT
    INSTITUTION_CD,
    PERSON_ID,
    AID_YEAR,
    ACAD_CAR_CD,
    'CS90' AS SRC_SYS_ID,
    INSTITUTION_SID,
    PERSON_SID,
    ACAD_CAR_SID,
    FED_NEED,
    AWARD_AMOUNT,
    GRANT_AID,
    GRANT_AID_DECLINED,
    GRANT_AID_NEED,
    GRANT_AID_NO_NEED,
    WORK_AID,
    WORK_AID_DECLINED,
    WORK_AID_NEED,
    WORK_AID_NO_NEED,
    LOAN_AID,
    LOAN_AID_DECLINED,
    LOAN_AID_NEED,
    LOAN_AID_NO_NEED,
    (GRANT_AID_NEED + WORK_AID_NEED + LOAN_AID_NEED) AS TOTAL_PKG_NEED,
    (GRANT_AID_NO_NEED + WORK_AID_NO_NEED + LOAN_AID_NO_NEED) AS TOTAL_PKG_NO_NEED,
    G_FED_AID,
    G_FED_AID_DECLINED,
    G_FED_AID_NEED,
    G_FED_AID_NO_NEED,
    G_STATE_AID,
    G_STATE_AID_DECLINED,
    G_STATE_AID_NEED,
    G_STATE_AID_NO_NEED,
    G_INSTIT_AID,
    G_INSTIT_AID_DECLINED,
    G_INSTIT_AID_NEED,
    G_INSTIT_AID_NO_NEED,
    O_WAIV_AID,
    O_WAIV_AID_DECLINED,
    O_WAIV_AID_NEED,
    O_WAIV_AID_NO_NEED,
    ATHL_AID,
    ATHL_AID_DECLINED,
    ATHL_AID_NEED,
    ATHL_AID_NO_NEED,
    G_PRVTE_AID,
    G_PRVTE_AID_DECLINED,
    G_PRVTE_AID_NEED,
    G_PRVTE_AID_NO_NEED,
    SH_CWP_AID,
    SH_CWP_AID_DECLINED,
    SH_CWP_AID_NEED,
    SH_CWP_AID_NO_NEED,
    SH_FWS_AID,
    SH_FWS_AID_DECLINED,
    SH_FWS_AID_NEED,
    SH_FWS_AID_NO_NEED,
    SH_LOAN_AID,
    SH_LOAN_AID_DECLINED,
    SH_SLOAN_AID_NEED,
    SH_SLOAN_AID_NO_NEED,
    O_PLOAN_AID,
    O_PLOAN_AID_DECLINED,
    O_PLOAN_AID_NEED,
    O_PLOAN_AID_NO_NEED,
    SUBSTR(INSTITUTION_CD, 1, 3)||AID_YEAR AS PARTITION_KEY,
    'S' AS DATA_ORIGIN,
    SYSDATE AS CREATED_EW_DTTM,
    SYSDATE AS LASTUPD_EW_DTTM
FROM T4;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_F_FA_AWARD_SUMM_BY_YEAR rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_F_FA_AWARD_SUMM_BY_YEAR',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_F_FA_AWARD_SUMM_BY_YEAR';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_F_FA_AWARD_SUMM_BY_YEAR enable constraint PK_UM_F_FA_AWARD_SUMM_BY_YEAR';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_F_FA_AWARD_SUMM_BY_YEAR');

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

END UM_F_FA_AWARD_SUMM_BY_YEAR_P;
/
