DROP PROCEDURE CSMRT_OWNER.UM_M_AF_STDNT_SF_DETAIL_P
/

--
-- UM_M_AF_STDNT_SF_DETAIL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_M_AF_STDNT_SF_DETAIL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--Created                    -- Smitha Paul
--Date                       -- 3/30/2022
--Loads table                -- UM_M_AF_STDNT_SF_DETAIL


------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_M_AF_STDNT_SF_DETAIL';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_M_AF_STDNT_SF_DETAIL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_M_AF_STDNT_SF_DETAIL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_M_AF_STDNT_SF_DETAIL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_M_AF_STDNT_SF_DETAIL');


strMessage01    := 'Inserting data into CSMRT_OWNER.UM_M_AF_STDNT_SF_DETAIL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_M_AF_STDNT_SF_DETAIL';				
insert /*+ append enable_parallel_dml parallel(8) */ into CSMRT_OWNER.UM_M_AF_STDNT_SF_DETAIL 
--BDL Data
 select * from (
  WITH
        SF
        AS
            (SELECT /*+PARALLEL(8) inline no_merge
                                 USE_HASH(SF_ITEM SF_ITEM_TYPE SF_ACC FA_ITEM_TYPE)
                                        */
                    SF_ITEM.INSTITUTION_CD,
                    SF_ITEM.ACAD_CAR_CD,
                    SF_ITEM.PERSON_ID,
                    to_CHAR(DTDIM.FISCAL_YEAR) AS FISCAL_YEAR,
                    SF_ITEM.ITEM_TERM,
                    SF_ITEM.ITEM_TERM_LD,
                    SF_ITEM.ITEM_TYPE,
                    SF_ITEM.ITEM_NBR,
                    SF_ITEM.ITEM_AMT,
                    SF_ITEM_TYPE.ITEM_TYPE_LD,
                    SF_ITEM_TYPE.KEYWORD1,
                    SF_ITEM_TYPE.KEYWORD2,
                    SF_ITEM_TYPE.KEYWORD3,
                    SF_ACC.RUN_DT,
                    SF_ACC.SEQNUM,
                    SF_ACC.SF_LINE_NBR,
                    SF_ACC.JOURNAL_DATE,
                    SF_ACC.ACCOUNT,
                    SF_ACC.FUND_CODE,
                    SF_ACC.DEPTID,
                    SF_ACC.MONETARY_AMOUNT,
                    SF_ACC.LINE_DESCR,
                    SF_ACC.JOURNAL_LINE_DATE,
                    SF_ACC.ACCOUNTING_DT,
                    SF_ACC.LEDGER,
                    FA_ITEM_TYPE.NEED_BASED,
                    FA_ITEM_TYPE.FA_SOURCE,
                    FA_ITEM_TYPE.FA_SOURCE_LD,
                    FA_ITEM_TYPE.FIN_AID_TYPE,
                    FA_ITEM_TYPE.FIN_AID_TYPE_LD,
                    FA_ITEM_TYPE.AGGREGATE_AREA,
                    COUNT (DISTINCT SF_ITEM.ACAD_CAR_CD)
                        OVER (PARTITION BY SF_ITEM.INSTITUTION_CD,
                                           SF_ITEM.PERSON_ID,
                                           SF_ITEM.ITEM_TERM,
                                           to_CHAR(DTDIM.FISCAL_YEAR))    AS CAREER_CNT
               FROM CSMRT_OWNER.UM_F_SF_ITEM  SF_ITEM
                    INNER JOIN CSMRT_OWNER.UM_D_ITEM_TYPE_VW SF_ITEM_TYPE
                        ON (    SF_ITEM.ITEM_TYPE = SF_ITEM_TYPE.ITEM_TYPE_ID
                            AND SF_ITEM.INSTITUTION_CD = SF_ITEM_TYPE.SETID)
                    INNER JOIN CSSTG_OWNER.PS_SF_ACCTG_LN SF_ACC
                        ON (    SF_ITEM.INSTITUTION_CD =
                                SF_ACC.BUSINESS_UNIT_GL
                            AND SF_ITEM.ITEM_NBR = SF_ACC.ITEM_NBR
                            AND SF_ITEM.PERSON_ID = SF_ACC.COMMON_ID
                            AND SF_ITEM.ITEM_TERM = SF_ACC.ITEM_TERM
                            AND SF_ITEM.ITEM_TYPE = SF_ACC.ITEM_TYPE)
                    INNER JOIN COMMON_OWNER.DATE_DIM DTDIM
                        ON (DTDIM.CALENDAR_DATE = SF_ACC.JOURNAL_DATE)
                    LEFT JOIN CSMRT_OWNER.UM_D_FA_ITEM_TYPE_VW FA_ITEM_TYPE
                        ON (    FA_ITEM_TYPE.INSTITUTION_CD =
                                SF_ITEM.INSTITUTION_CD
                            AND FA_ITEM_TYPE.ITEM_TYPE = SF_ITEM.ITEM_TYPE
                            AND to_CHAR(DTDIM.FISCAL_YEAR) = to_CHAR(FA_ITEM_TYPE.AID_YEAR))
              WHERE     to_CHAR(DTDIM.FISCAL_YEAR) >= '2019'
                    AND SF_ITEM.INSTITUTION_CD IN ('UMDAR', 'UMLOW', 'UMBOS')
                    AND (SF_ACC.ACCOUNT LIKE '6%' OR SF_ACC.ACCOUNT LIKE '7%')),
        CAR_TERM
        AS
            ( /*Get career to fill out blank rows when a student has more than 1 career as well as blank career rows*/
             SELECT DISTINCT
                    INSTITUTION_CD,
                    TERM_CD,
                    PERSON_ID,
                    ACAD_CAR_CD,
                    MAX (TERM_CD)
                        OVER (PARTITION BY INSTITUTION_CD, PERSON_ID)    AS MAX_TERM
               FROM CSMRT_OWNER.UM_F_STDNT_ACAD_STRUCT_VW
              WHERE PRIM_STACK_STDNT_RANK = 1),
        SF_T1
        AS
            (SELECT                                                 --DISTINCT
                    SF.INSTITUTION_CD,
                    /*Acad Car cd is not always filled out in all rows. When a student has 2 distinct career rows in one item term where one row is '-',
                      then it can be safely assumed that the other row holds their actual career. This code replaces the '-' career for such students
                      with the actual career. We currently do not do anything for students that have 3 disticnt careers in a term where one is '-'
                    */
                    CASE
                        WHEN (SF.CAREER_CNT = 2 AND SF.ACAD_CAR_CD = '-')
                        THEN
                            (MAX (SF.ACAD_CAR_CD)
                                 OVER (
                                     PARTITION BY SF.INSTITUTION_CD,
                                                  SF.PERSON_ID,
                                                  SF.ITEM_TERM))
                        ELSE
                            (SF.ACAD_CAR_CD)
                    END    AS ACAD_CAR_CD,
                    SF.PERSON_ID,
                    to_CHAR(SF.FISCAL_YEAR) AS FISCAL_YEAR,
                    SF.ITEM_TERM,
                    SF.ITEM_TERM_LD,
                    SF.ITEM_TYPE,
                    SF.ITEM_TYPE_LD,
                    SF.KEYWORD1,
                    SF.KEYWORD2,
                    SF.KEYWORD3,
                    SF.ITEM_NBR,
                    SF.ITEM_AMT,
                    SF.RUN_DT,
                    SF.SEQNUM,
                    SF.SF_LINE_NBR,
                    SF.JOURNAL_DATE,
                    SF.ACCOUNT,
                    SF.FUND_CODE,
                    SF.DEPTID,
                    SF.MONETARY_AMOUNT,
                    SF.LINE_DESCR,
                    SF.JOURNAL_LINE_DATE,
                    SF.ACCOUNTING_DT,
                    SF.LEDGER,
                    SF.NEED_BASED,
                    SF.FA_SOURCE,
                    SF.FA_SOURCE_LD,
                    SF.FIN_AID_TYPE,
                    SF.FIN_AID_TYPE_LD,
                    SF.AGGREGATE_AREA,
                    CASE
                        WHEN SF.KEYWORD1 IN ('DCE',
                                             'DCE FEES',
                                             'FEES',
                                             'MANDFEES')
                        THEN
                            'Fees'
                        WHEN SF.KEYWORD1 IN ('DCE TUIT', 'TUITION')
                        THEN
                            'Tuition'
                        WHEN SF.KEYWORD1 = 'FINAID' AND SF.KEYWORD2 = 'GRANT'
                        THEN
                            'Grants'
                        WHEN SF.KEYWORD1 = 'FINAID' AND SF.KEYWORD2 = 'LOAN'
                        THEN
                            'Loans'
                        WHEN     SF.KEYWORD1 = 'FINAID'
                             AND SF.KEYWORD2 = 'SCHOLAR'
                        THEN
                            'Scholarships'
                        WHEN     SF.KEYWORD1 = 'FINAID'
                             AND SF.KEYWORD2 = 'WAIVER'
                        THEN
                            'Waivers'
                        WHEN SF.KEYWORD1 = 'HOUSING' AND SF.KEYWORD2 = '-'
                        THEN
                            'Auxiliary'
                        WHEN     SF.KEYWORD1 = 'PAYMENTS'
                             AND SF.KEYWORD2 = 'SFSCHOL'
                        THEN
                            'Scholarships'
                        WHEN SF.KEYWORD1 = 'WAIVERS'
                        THEN
                            'Waivers'
                        WHEN     SF.KEYWORD1 = '-'
                             AND SF.KEYWORD2 = '-'
                             AND SF.ITEM_TYPE_LD = 'ATI Testing Fee'
                        THEN
                            'Fees'
                        WHEN     SF.KEYWORD1 = 'FINAID'
                             AND SF.KEYWORD2 = 'SUMMER'
                             AND SF.ITEM_TYPE_LD LIKE '%Grant%'
                        THEN
                            'Grants'
                        WHEN     SF.KEYWORD1 = 'FINAID'
                             AND SF.KEYWORD2 = 'SUMMER'
                             AND SF.ITEM_TYPE_LD LIKE '%Loan%'
                        THEN
                            'Loans'
                        WHEN     SF.KEYWORD1 = 'PATHWAYS'
                             AND SF.KEYWORD2 = '-'
                             AND SF.ITEM_TYPE_LD LIKE '%Fee%'
                        THEN
                            'Fees'
                        WHEN     SF.KEYWORD1 = 'PATHWAYS'
                             AND SF.KEYWORD2 = '-'
                             AND SF.ITEM_TYPE_LD LIKE
                                     '%Connect CAS Humanities%'
                        THEN
                            'Fees'
                        WHEN     SF.KEYWORD1 = 'PATHWAYS'
                             AND SF.KEYWORD2 = '-'
                             AND SF.ITEM_TYPE_LD LIKE
                                     '%Connect Visual%Perf. Arts F%'
                        THEN
                            'Fees'
                        WHEN     SF.KEYWORD1 = 'PATHWAYS'
                             AND SF.KEYWORD2 = '-'
                             AND SF.ITEM_TYPE_LD LIKE '%Tuition%'
                        THEN
                            'Tuition'
                        WHEN     SF.KEYWORD1 = 'CE'
                             AND SF.KEYWORD2 = '-'
                             AND SF.ITEM_TYPE_LD LIKE '%Tuition%'
                        THEN
                            'Tuition'
                        WHEN     SF.KEYWORD1 = 'CE'
                             AND SF.KEYWORD2 = '-'
                             AND SF.ITEM_TYPE_LD LIKE '%Fee%'
                        THEN
                            'Fees'
                    END    AS SF_CATEGORY_CALC
               FROM SF)
    SELECT T0.INSTITUTION_CD,
           CASE
               WHEN (T0.ACAD_CAR_CD = '-' AND CAR_TERM.ACAD_CAR_CD IS NULL)
               THEN
                   (CT2.ACAD_CAR_CD)
               WHEN (    T0.ACAD_CAR_CD = '-'
                     AND CAR_TERM.ACAD_CAR_CD IS NOT NULL)
               THEN
                   (CAR_TERM.ACAD_CAR_CD)
               ELSE
                   (T0.ACAD_CAR_CD)
           END    AS ACAD_CAR_CD,
           T0.PERSON_ID,
           to_CHAR(T0.FISCAL_YEAR),
           T0.ITEM_TERM,
           T0.ITEM_TERM_LD,
           T0.ITEM_TYPE,
           T0.ITEM_TYPE_LD,
           T0.KEYWORD1,
           T0.KEYWORD2,
           T0.KEYWORD3,
           T0.ITEM_NBR,
           T0.ITEM_AMT,
           T0.RUN_DT,
           T0.SEQNUM,
           T0.SF_LINE_NBR,
           T0.JOURNAL_DATE,
           T0.ACCOUNT,
           T0.FUND_CODE,
           T0.DEPTID,
           T0.MONETARY_AMOUNT,
           T0.LINE_DESCR,
           T0.JOURNAL_LINE_DATE,
           T0.ACCOUNTING_DT,
           T0.LEDGER,
           T0.NEED_BASED,
           T0.FA_SOURCE,
           T0.FA_SOURCE_LD,
           T0.FIN_AID_TYPE,
           T0.FIN_AID_TYPE_LD,
           T0.AGGREGATE_AREA,
           T0.SF_CATEGORY_CALC,
           CASE
               WHEN (    FA_SOURCE IS NOT NULL
                     AND FA_SOURCE <> 'O'
                     AND SF_CATEGORY_CALC <> 'Loans')
               THEN
                   FA_SOURCE_LD
               WHEN (    (FA_SOURCE IS NULL OR FA_SOURCE = 'O')
                     AND KEYWORD3 = 'FEDERAL'
                     AND SF_CATEGORY_CALC <> 'Loans')
               THEN
                   'Federal'
               WHEN (    (FA_SOURCE IS NULL OR FA_SOURCE = 'O')
                     AND KEYWORD3 = 'STATE'
                     AND SF_CATEGORY_CALC <> 'Loans')
               THEN
                   'State'
               WHEN (    (FA_SOURCE IS NULL OR FA_SOURCE = 'O')
                     AND KEYWORD3 = 'INSTITUTNL'
                     AND SF_CATEGORY_CALC <> 'Loans')
               THEN
                   'Institutional'
               WHEN (    (FA_SOURCE IS NULL OR FA_SOURCE = 'O')
                     AND KEYWORD3 = 'PRIVATE'
                     AND SF_CATEGORY_CALC <> 'Loans')
               THEN
                   'Private'
               WHEN (    (FA_SOURCE IS NULL OR FA_SOURCE = 'O')
                     AND KEYWORD3 NOT IN ('FEDERAL',
                                          'STATE',
                                          'INSTITUTNL',
                                          'PRIVATE')
                     AND SF_CATEGORY_CALC = 'Scholarships')
               THEN
                   'Private'
               WHEN (    (FA_SOURCE IS NULL OR FA_SOURCE = 'O')
                     AND KEYWORD3 NOT IN ('FEDERAL',
                                          'STATE',
                                          'INSTITUTNL',
                                          'PRIVATE')
                     AND SF_CATEGORY_CALC = 'Waivers')
               THEN
                   'Institutional'
               ELSE
                   'Other'
           END    AS FA_SOURCE_CALC,
           sysdate as insert_time
      FROM SF_T1  T0
           LEFT OUTER JOIN CAR_TERM /*Get highest precedence career in term to fill in blank career in SF*/
               ON     T0.INSTITUTION_CD = CAR_TERM.INSTITUTION_CD
                  AND CASE
                          WHEN (T0.ACAD_CAR_CD = '-')
                          THEN
                              (CAR_TERM.ACAD_CAR_CD)
                          ELSE
                              (T0.ACAD_CAR_CD)
                      END =
                      CAR_TERM.ACAD_CAR_CD
                  AND T0.PERSON_ID = CAR_TERM.PERSON_ID
                  AND T0.ITEM_TERM = CAR_TERM.TERM_CD
           LEFT OUTER JOIN CAR_TERM CT2 /*Get latest career for student to fill in blank career in SF*/
               ON     T0.INSTITUTION_CD = CT2.INSTITUTION_CD
                  AND T0.PERSON_ID = CT2.PERSON_ID
                  AND CT2.TERM_CD = CT2.MAX_TERM
       )           
                  
                  
UNION ALL

--AMH Data
select * from (
    WITH
        SF
        AS
            (SELECT /*+PARALLEL(8) inline no_merge USE_HASH(A B C E T2)*/
                    A.BUSINESS_UNIT,
                    A.ACAD_CAREER,
                    A.EMPLID,
                    to_CHAR(DTDIM.FISCAL_YEAR) AS FISCAL_YEAR,
                    A.ITEM_TERM,
                    T2.DESCR                             AS ITEM_TERM_LD,
                    A.ITEM_TYPE,
                    A.ITEM_NBR,
                    A.ITEM_AMT,
                    B.DESCR                              AS ITEM_TYPE_LD,
                    B.KEYWORD1,
                    B.KEYWORD2,
                    B.KEYWORD3,
                    C.RUN_DT,
                    C.SEQNUM,
                    C.SF_LINE_NBR,
                    C.JOURNAL_DATE,
                    C.ACCOUNT,
                    C.FUND_CODE,
                    C.DEPTID,
                    C.MONETARY_AMOUNT,
                    C.LINE_DESCR,
                    C.JOURNAL_LINE_DATE,
                    C.ACCOUNTING_DT,
                    C.LEDGER,
                    COUNT (DISTINCT A.ACAD_CAREER)
                        OVER (
                            PARTITION BY A.BUSINESS_UNIT,
                                         A.EMPLID,
                                         A.ITEM_TERM)    AS CAREER_CNT,
                    TRIM (E.UM_KEY_NODE1)                AS UM_KEY_NODE1
               FROM AMSTG_OWNER.PS_ITEM_SF_VW  A
                    INNER JOIN
                    (SELECT *
                       FROM AMSTG_OWNER.PS_ITEM_TYPE_TBL_VW B
                      WHERE B.EFFDT =
                            (SELECT MAX (B_ED.EFFDT)
                               FROM AMSTG_OWNER.PS_ITEM_TYPE_TBL_VW B_ED
                              WHERE     B.SETID = B_ED.SETID
                                    AND B.ITEM_TYPE = B_ED.ITEM_TYPE
                                    AND B_ED.EFFDT <= SYSDATE)) B
                        ON     A.ITEM_TYPE = B.ITEM_TYPE
                           AND A.BUSINESS_UNIT = B.SETID
                           AND A.BUSINESS_UNIT = 'UMAMH'
                           AND B.DATA_ORIGIN <> 'D'
                    INNER JOIN AMSTG_OWNER.PS_SF_ACCTG_LN_VW C
                        ON     A.BUSINESS_UNIT = C.BUSINESS_UNIT_GL
                           AND A.ITEM_NBR = C.ITEM_NBR
                           AND A.EMPLID = C.COMMON_ID
                           AND A.ITEM_TERM = C.ITEM_TERM
                           AND A.ITEM_TYPE = C.ITEM_TYPE
                           AND C.DATA_ORIGIN <> 'D'
                    INNER JOIN COMMON_OWNER.DATE_DIM DTDIM
                        ON DTDIM.CALENDAR_DATE = C.JOURNAL_DATE
                    INNER JOIN AMSTG_OWNER.PS_UM_RPT_NODES_VW E
                        ON E.ITEM_TYPE = A.ITEM_TYPE
                    LEFT OUTER JOIN AMSTG_OWNER.PS_TERM_VAL_TBL_VW T2
                        ON     A.ITEM_TERM = T2.STRM
                           AND A.SRC_SYS_ID = T2.SRC_SYS_ID
                           AND T2.DATA_ORIGIN <> 'D'
              WHERE     to_CHAR(DTDIM.FISCAL_YEAR) >= '2019'
                    AND (C.ACCOUNT LIKE '6%' OR C.ACCOUNT LIKE '7%')
                    /*AND C.JOURNAL_DATE BETWEEN TO_DATE ('07/01/2019',
                                                        'MM/DD/YYYY')
                                           AND TO_DATE ('06/30/2020',
                                                        'MM/DD/YYYY')*/
                    AND A.DATA_ORIGIN <> 'D'),
        FA_ITEM_TYPE
        AS
            (SELECT *
               FROM AMSTG_OWNER.PS_ITEM_TYPE_FA_VW
              --WHERE AID_YEAR = '2020'
              WHERE DATA_ORIGIN <> 'D'),
        X
        AS
            (SELECT /*+PARALLEL(8) inline*/
                    FIELDNAME,
                    FIELDVALUE,
                    EFFDT,
                    SRC_SYS_ID,
                    XLATLONGNAME,
                    XLATSHORTNAME,
                    DATA_ORIGIN,
                    ROW_NUMBER ()
                        OVER (
                            PARTITION BY FIELDNAME, FIELDVALUE, SRC_SYS_ID
                            ORDER BY
                                DATA_ORIGIN DESC,
                                (CASE
                                     WHEN EFFDT > TRUNC (SYSDATE)
                                     THEN
                                         TO_DATE ('01-JAN-1800')
                                     ELSE
                                         EFFDT
                                 END) DESC)    X_ORDER
               FROM AMSTG_OWNER.PSXLATITEM_VW
              WHERE DATA_ORIGIN <> 'D')
    SELECT /*+PARALLEL(8) inline*/
            DISTINCT
           SF.BUSINESS_UNIT,
           --SF.ACAD_CAREER                AS ACAD_CAREER_ORIG,
           CASE
               WHEN (SF.CAREER_CNT = 2 AND SF.ACAD_CAREER = '-')
               THEN
                   (MAX (SF.ACAD_CAREER)
                        OVER (
                            PARTITION BY SF.BUSINESS_UNIT,
                                         SF.EMPLID,
                                         SF.ITEM_TERM))
               ELSE
                   (SF.ACAD_CAREER)
           END                           AS ACAD_CAREER,
           SF.EMPLID,
           to_CHAR(SF.FISCAL_YEAR),
           SF.ITEM_TERM,
           SF.ITEM_TERM_LD,
           SF.ITEM_TYPE,
           SF.ITEM_TYPE_LD,
           SF.KEYWORD1,
           SF.KEYWORD2,
           SF.KEYWORD3,
           SF.ITEM_NBR,
           SF.ITEM_AMT,
           SF.RUN_DT,
           SF.SEQNUM,
           SF.SF_LINE_NBR,
           SF.JOURNAL_DATE,
           SF.ACCOUNT,
           SF.FUND_CODE,
           SF.DEPTID,
           SF.MONETARY_AMOUNT,
           SF.LINE_DESCR,
           SF.JOURNAL_LINE_DATE,
           SF.ACCOUNTING_DT,
           SF.LEDGER,
           FA_ITEM_TYPE.NEED_BASED,
           FA_ITEM_TYPE.FA_SOURCE,
           NVL (X1.XLATLONGNAME, '-')    FA_SOURCE_LD,
           FA_ITEM_TYPE.FIN_AID_TYPE,
           NVL (X4.XLATLONGNAME, '-')    FIN_AID_TYPE_LD,
           FA_ITEM_TYPE.AGGREGATE_AREA,
                     CASE
                         WHEN UM_KEY_NODE1 IN ('HEERF',
                                               'Other Scholarship',
                                               'Private Scholarship',
                                               'Institutional Schlp')
                         THEN
                             ('Scholarships')
                         WHEN UM_KEY_NODE1 IN
                                  ('CPE Fees', 'Other Fees', 'Mandatory Fee')
                         THEN
                             ('Fees')
                         WHEN UM_KEY_NODE1 IN ('Federal Grant',
                                               'Institutional Grant',
                                               'State Grant',
                                               'Other Grants',
                                               'Grant - Other')
                         THEN
                             ('Grants')
                         WHEN UM_KEY_NODE1 IN ('Loan')
                         THEN
                             ('Loan')
                         WHEN UM_KEY_NODE1 IN ('DNR')
                         THEN
                             ('Other')
                         WHEN UM_KEY_NODE1 IN ('Housing', 'Dining')
                         THEN
                             ('Auxiliary')
                         WHEN UM_KEY_NODE1 IN ('CPE Tuition', 'Tuition')
                         THEN
                             ('Tuition')
                         WHEN UM_KEY_NODE1 IN ('Waiver')
                         THEN
                             ('Waivers')
                         ELSE
                             ('OTHER')
                     END                           AS SF_CATEGORY_CALC,
                     CASE
                         WHEN FA_ITEM_TYPE.FA_SOURCE = 'F'
                         THEN
                             ('Federal')
                         WHEN     FA_ITEM_TYPE.FA_SOURCE = 'O'
                              AND UM_KEY_NODE1 IN
                                      ('Grant - Other',
                                       'Other Scholarship',
                                       'Private Scholarship')
                         THEN
                             ('Federal')
                         WHEN     FA_ITEM_TYPE.FA_SOURCE = 'P'
                              AND UM_KEY_NODE1 IN ('Other Scholarship')
                         THEN
                             ('Federal')
                         WHEN     FA_ITEM_TYPE.FA_SOURCE = '-'
                              AND UM_KEY_NODE1 IN ('HEERF')
                         THEN
                             ('Federal')
                         WHEN FA_ITEM_TYPE.FA_SOURCE = 'I'
                         THEN
                             ('Institutional')
                         WHEN     FA_ITEM_TYPE.FA_SOURCE = 'P'
                              AND UM_KEY_NODE1 IN ('Institutional Grant')
                         THEN
                             ('Institutional')
                         WHEN     FA_ITEM_TYPE.FA_SOURCE = '-'
                              AND UM_KEY_NODE1 IN ('Waiver')
                         THEN
                             ('Institutional')
                         WHEN     FA_ITEM_TYPE.FA_SOURCE = 'P'
                              AND UM_KEY_NODE1 NOT IN
                                      ('Institutional Grant',
                                       'Other Scholarship')
                         THEN
                             ('Private')
                         WHEN     FA_ITEM_TYPE.FA_SOURCE = 'P'
                              AND UM_KEY_NODE1 IN ('Institutional Grant')
                         THEN
                             ('Private')
                         WHEN     FA_ITEM_TYPE.FA_SOURCE = '-'
                              AND UM_KEY_NODE1 IN ('Grant - Other')
                         THEN
                             ('Private')
                         WHEN FA_ITEM_TYPE.FA_SOURCE = 'S'
                         THEN
                             ('State')
                         ELSE
                             ('OTHER')
                     END                           AS FA_SOURCE_CALC,
                     sysdate as insert_time
                     
      FROM SF
           LEFT OUTER JOIN FA_ITEM_TYPE
               ON     SF.BUSINESS_UNIT = FA_ITEM_TYPE.SETID
                  AND SF.ITEM_TYPE = FA_ITEM_TYPE.ITEM_TYPE
                  AND SF.FISCAL_YEAR = FA_ITEM_TYPE.AID_YEAR
           LEFT OUTER JOIN X X1
               ON     FA_ITEM_TYPE.FA_SOURCE = X1.FIELDVALUE
                  AND FA_ITEM_TYPE.SRC_SYS_ID = X1.SRC_SYS_ID
                  AND X1.FIELDNAME = 'FA_SOURCE'
                  AND X1.X_ORDER = 1
           LEFT OUTER JOIN X X4
               ON     FA_ITEM_TYPE.FIN_AID_TYPE = X4.FIELDVALUE
                  AND FA_ITEM_TYPE.SRC_SYS_ID = X4.SRC_SYS_ID
                  AND X4.FIELDNAME = 'FIN_AID_TYPE'
                  AND X4.X_ORDER = 1
                  
                  );

           
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_M_AF_STDNT_SF_DETAIL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_M_AF_STDNT_SF_DETAIL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_M_AF_STDNT_SF_DETAIL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_M_AF_STDNT_SF_DETAIL');

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

END UM_M_AF_STDNT_SF_DETAIL_P;
/
