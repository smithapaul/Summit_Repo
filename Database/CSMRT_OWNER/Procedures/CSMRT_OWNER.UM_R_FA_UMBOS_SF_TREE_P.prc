DROP PROCEDURE CSMRT_OWNER.UM_R_FA_UMBOS_SF_TREE_P
/

--
-- UM_R_FA_UMBOS_SF_TREE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_R_FA_UMBOS_SF_TREE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table UM_R_FA_UMBOS_SF_TREE from PeopleSoft table UM_R_FA_UMBOS_SF_TREE.
--
 --V01  SMT-xxxx 06/28/2018,    James Doucette
--                              Converted from SQL Script
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_R_FA_UMBOS_SF_TREE';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_R_FA_UMBOS_SF_TREE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_R_FA_UMBOS_SF_TREE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_R_FA_UMBOS_SF_TREE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_R_FA_UMBOS_SF_TREE');

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_FA_UMBOS_SF_TREE disable constraint PK_UM_R_FA_UMBOS_SF_TREE';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_R_FA_UMBOS_SF_TREE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_R_FA_UMBOS_SF_TREE';
insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.UM_R_FA_UMBOS_SF_TREE
    WITH
        Q1
        AS
            (SELECT /*+ inline */
                    SETID,
                    ITEM_TYPE,
                    SRC_SYS_ID,
                    EFFDT,
                    EFF_STATUS,
                    DESCR,
                    ROW_NUMBER ()
                        OVER (
                            PARTITION BY SETID, ITEM_TYPE, SRC_SYS_ID
                            ORDER BY
                                DATA_ORIGIN DESC,
                                (CASE
                                     WHEN EFFDT > TRUNC (SYSDATE)
                                     THEN
                                         TO_DATE ('01-JAN-1900')
                                     ELSE
                                         EFFDT
                                 END) DESC)    Q_ORDER
               FROM CSSTG_OWNER.PS_ITEM_TYPE_TBL
              WHERE DATA_ORIGIN <> 'D' AND SETID = 'UMBOS'),
        Q2
        AS
            (SELECT /*+ inline */
                    SETID,
                    SETCNTRLVALUE,
                    TREE_NAME,
                    TREE_NODE_NUM,
                    TREE_NODE,
                    TREE_BRANCH,
                    SRC_SYS_ID,
                    EFFDT,
                    ROW_NUMBER ()
                        OVER (
                            PARTITION BY SETID,
                                         SETCNTRLVALUE,
                                         TREE_NAME,
                                         TREE_NODE_NUM,
                                         TREE_NODE,
                                         TREE_BRANCH,
                                         SRC_SYS_ID
                            ORDER BY
                                DATA_ORIGIN DESC,
                                (CASE
                                     WHEN EFFDT > TRUNC (SYSDATE)
                                     THEN
                                         TO_DATE ('01-JAN-1900')
                                     ELSE
                                         EFFDT
                                 END) DESC)    Q_ORDER
               FROM CSSTG_OWNER.PSTREENODE
              WHERE     DATA_ORIGIN <> 'D'
                    AND SETID = 'UMBOS'
                    AND TREE_NAME = 'FA_MISC_REPORTS'
                    AND TREE_NODE <> 'ALL'),
        Q3
        AS
            (SELECT /*+ inline */
                    SETID,
                    SETCNTRLVALUE,
                    TREE_NAME,
                    TREE_NODE_NUM,
                    RANGE_FROM,
                    RANGE_TO,
                    TREE_BRANCH,
                    SRC_SYS_ID,
                    EFFDT,
                    DATA_ORIGIN,
                    ROW_NUMBER ()
                        OVER (
                            PARTITION BY SETID,
                                         SETCNTRLVALUE,
                                         TREE_NAME,
                                         TREE_NODE_NUM,
                                         RANGE_FROM,
                                         RANGE_TO,
                                         TREE_BRANCH,
                                         SRC_SYS_ID
                            ORDER BY
                                DATA_ORIGIN DESC,
                                (CASE
                                     WHEN EFFDT > TRUNC (SYSDATE)
                                     THEN
                                         TO_DATE ('01-JAN-1900')
                                     ELSE
                                         EFFDT
                                 END) DESC)    Q_ORDER
               FROM CSSTG_OWNER.PSTREELEAF
              WHERE     DATA_ORIGIN <> 'D'
                    AND SETID = 'UMBOS'
                    AND TREE_NAME = 'FA_MISC_REPORTS'),
        Q4
        AS
            (SELECT /*+ inline no_use_nl(Q1 Q2 Q3) */
                    Q2.SETID,
                    Q2.TREE_NAME,
                    Q2.TREE_NODE,
                    Q2.TREE_NODE_NUM,
                    Q1.ITEM_TYPE,
                    Q1.DESCR,
                    Q2.SRC_SYS_ID
               FROM Q2
                    JOIN Q3
                        ON     Q2.SETID = Q3.SETID
                           AND Q2.SETCNTRLVALUE = Q3.SETCNTRLVALUE
                           AND Q2.TREE_NAME = Q3.TREE_NAME
                           AND Q2.TREE_NODE_NUM = Q3.TREE_NODE_NUM
                           AND Q2.TREE_BRANCH = Q3.TREE_BRANCH
                           AND Q2.SRC_SYS_ID = Q3.SRC_SYS_ID
                           AND Q3.Q_ORDER = 1
                    JOIN Q1
                        ON     Q3.SETID = Q1.SETID
                           AND Q1.ITEM_TYPE BETWEEN Q3.RANGE_FROM
                                                AND Q3.RANGE_TO
                           AND Q3.SRC_SYS_ID = Q1.SRC_SYS_ID
                           AND Q1.Q_ORDER = 1
              WHERE Q2.Q_ORDER = 1),
        Q5
        AS
            (SELECT /*+ inline */
                    SETID,
                    WAIVER_CODE,
                    SRC_SYS_ID,
                    EFFDT,
                    EFF_STATUS,
                    ACCOUNT_TYPE_SF,
                    ITEM_TYPE,
                    CRITERIA,
                    ROW_NUMBER ()
                        OVER (
                            PARTITION BY SETID, WAIVER_CODE, SRC_SYS_ID
                            ORDER BY
                                DATA_ORIGIN DESC,
                                (CASE
                                     WHEN EFFDT > TRUNC (SYSDATE)
                                     THEN
                                         TO_DATE ('01-JAN-1900')
                                     ELSE
                                         EFFDT
                                 END) DESC,
                                EFF_STATUS)    Q_ORDER
               FROM CSSTG_OWNER.PS_WAIVER_TBL
              WHERE DATA_ORIGIN <> 'D' AND SETID = 'UMBOS'),
        Q6
        AS
            (SELECT /*+ inline no_use_nl(Q5 C V) */
                    DISTINCT Q5.SETID,
                             Q5.WAIVER_CODE,
                             Q5.EFFDT,
                             Q5.EFF_STATUS,
                             Q5.ACCOUNT_TYPE_SF,
                             Q5.ITEM_TYPE,
                             V.SELECT_VALUE,
                             Q5.SRC_SYS_ID
               FROM Q5
                    JOIN CSSTG_OWNER.PS_SEL_CRITER_TBL C
                        ON     Q5.SETID = C.BUSINESS_UNIT
                           AND Q5.CRITERIA = C.CRITERIA
                           AND Q5.SRC_SYS_ID = C.SRC_SYS_ID
                           AND C.FIELDNAME LIKE 'VARIABLE_CHAR%'
                    JOIN CSSTG_OWNER.PS_SEL_VALUE_TBL V
                        ON     C.BUSINESS_UNIT = V.BUSINESS_UNIT
                           AND C.CRITERIA = V.CRITERIA
                           AND C.EFFDT = V.EFFDT
                           AND C.SEQNO = V.SEQNO
                           AND C.SRC_SYS_ID = V.SRC_SYS_ID
              WHERE Q5.Q_ORDER = 1),
        Q7
        AS (
            SELECT /*+ inline parallel(8) */
                    INSTITUTION,
                    BILLING_CAREER,
                    STRM,
                    EMPLID,
                    EQTN_CD,
                    SRC_SYS_ID
               FROM (
            (SELECT
                    INSTITUTION,
                    BILLING_CAREER,
                    STRM,
                    EMPLID,
                    VARIABLE_CHAR1     EQTN_CD,
                    SRC_SYS_ID
               FROM CSSTG_OWNER.PS_STDNT_EQUTN_VAR
              WHERE     DATA_ORIGIN <> 'D'
                    AND INSTITUTION = 'UMBOS'
                    AND BILLING_CAREER = 'UGRD'
                    AND VARIABLE_CHAR1 <> '-'
             UNION
             SELECT
                    INSTITUTION,
                    BILLING_CAREER,
                    STRM,
                    EMPLID,
                    VARIABLE_CHAR2     EQTN_CD,
                    SRC_SYS_ID
               FROM CSSTG_OWNER.PS_STDNT_EQUTN_VAR
              WHERE     DATA_ORIGIN <> 'D'
                    AND INSTITUTION = 'UMBOS'
                    AND BILLING_CAREER = 'UGRD'
                    AND VARIABLE_CHAR2 <> '-'
             UNION
             SELECT
                    INSTITUTION,
                    BILLING_CAREER,
                    STRM,
                    EMPLID,
                    VARIABLE_CHAR3     EQTN_CD,
                    SRC_SYS_ID
               FROM CSSTG_OWNER.PS_STDNT_EQUTN_VAR
              WHERE     DATA_ORIGIN <> 'D'
                    AND INSTITUTION = 'UMBOS'
                    AND BILLING_CAREER = 'UGRD'
                    AND VARIABLE_CHAR3 <> '-'
             UNION
             SELECT
                    INSTITUTION,
                    BILLING_CAREER,
                    STRM,
                    EMPLID,
                    VARIABLE_CHAR4     EQTN_CD,
                    SRC_SYS_ID
               FROM CSSTG_OWNER.PS_STDNT_EQUTN_VAR
              WHERE     DATA_ORIGIN <> 'D'
                    AND INSTITUTION = 'UMBOS'
                    AND BILLING_CAREER = 'UGRD'
                    AND VARIABLE_CHAR4 <> '-'
             UNION
             SELECT
                    INSTITUTION,
                    BILLING_CAREER,
                    STRM,
                    EMPLID,
                    VARIABLE_CHAR5     EQTN_CD,
                    SRC_SYS_ID
               FROM CSSTG_OWNER.PS_STDNT_EQUTN_VAR
              WHERE     DATA_ORIGIN <> 'D'
                    AND INSTITUTION = 'UMBOS'
                    AND BILLING_CAREER = 'UGRD'
                    AND VARIABLE_CHAR5 <> '-'))),
        Q8
        AS
            (SELECT /*+ inline parallel(8) no_use_nl(Q7 Q6) */
                    DISTINCT Q7.INSTITUTION,
                             Q7.BILLING_CAREER,
                             Q7.STRM,
                             Q7.EMPLID,
                             Q6.SELECT_VALUE,
                             Q6.ACCOUNT_TYPE_SF,
                             Q6.WAIVER_CODE,
                             Q6.ITEM_TYPE,
                             Q7.SRC_SYS_ID,
                             Q6.EFFDT,
                             Q6.EFF_STATUS
               FROM Q7
                    JOIN Q6
                        ON     Q6.SETID = Q7.INSTITUTION
                           AND Q6.SELECT_VALUE = Q7.EQTN_CD
                           AND Q6.SRC_SYS_ID = Q7.SRC_SYS_ID)
    SELECT /*+ parallel(8) no_use_nl(Q8 Q4 T P) */
           Q8.INSTITUTION                     INSTITUTION_CD,
           Q8.BILLING_CAREER                  ACAD_CAR_CD,
           Q8.STRM                            TERM_CD,
           Q8.EMPLID                          PERSON_ID,
           Q8.SELECT_VALUE,
           Q8.ACCOUNT_TYPE_SF,
           Q8.WAIVER_CODE,
           Q8.ITEM_TYPE,
           Q4.TREE_NAME,
           Q4.TREE_NODE,
           Q4.TREE_NODE_NUM,
           Q8.SRC_SYS_ID,
           Q8.EFFDT,
           Q8.EFF_STATUS,
           Q4.DESCR,
           NVL (T.TERM_SID, 2147483646)       TERM_SID,
           NVL (P.PERSON_SID, 2147483646)     PERSON_SID,
           'S'                                DATA_ORIGIN,
           SYSDATE                            CREATED_EW_DTTM,
           SYSDATE                            LASTUPD_EW_DTTM
      FROM Q8
           JOIN Q4
               ON     Q4.SETID = Q8.INSTITUTION
                  AND Q4.ITEM_TYPE = Q8.ITEM_TYPE
                  AND Q4.SRC_SYS_ID = Q8.SRC_SYS_ID
           LEFT OUTER JOIN PS_D_TERM T
               ON     Q8.INSTITUTION = T.INSTITUTION_CD
                  AND Q8.BILLING_CAREER = T.ACAD_CAR_CD
                  AND Q8.STRM = T.TERM_CD
                  AND Q8.SRC_SYS_ID = T.SRC_SYS_ID
           LEFT OUTER JOIN PS_D_PERSON P
               ON Q8.EMPLID = P.PERSON_ID AND Q8.SRC_SYS_ID = T.SRC_SYS_ID
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_R_FA_UMBOS_SF_TREE rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_R_FA_UMBOS_SF_TREE',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_R_FA_UMBOS_SF_TREE',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_R_FA_UMBOS_SF_TREE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

--strSqlDynamic   := 'alter table CSMRT_OWNER.UM_R_FA_UMBOS_SF_TREE enable constraint PK_UM_R_FA_UMBOS_SF_TREE';
--strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
--COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
--                (
--                i_SqlStatement          => strSqlDynamic,
--                i_MaxTries              => 10,
--                i_WaitSeconds           => 10,
--                o_Tries                 => intTries
--                );

COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_R_FA_UMBOS_SF_TREE');

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

END UM_R_FA_UMBOS_SF_TREE_P;
/
