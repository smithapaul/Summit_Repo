DROP PROCEDURE DLMRT_OWNER.FIN_PLAN_FINANCE_P
/

--
-- FIN_PLAN_FINANCE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."FIN_PLAN_FINANCE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--Created                    -- Smitha Paul
--Date                       -- 4/13/2022
--Loads table                -- FIN_PLAN_FINANCE


------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'FIN_PLAN_FINANCE';
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

strMessage01    := 'Truncating table DLMRT_OWNER.FIN_PLAN_FINANCE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table DLMRT_OWNER.FIN_PLAN_FINANCE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table DLMRT_OWNER.FIN_PLAN_FINANCE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('DLMRT_OWNER','FIN_PLAN_FINANCE');


strMessage01    := 'Inserting data into DLMRT_OWNER.FIN_PLAN_FINANCE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into DLMRT_OWNER.FIN_PLAN_FINANCE';				
insert /*+ append enable_parallel_dml parallel(8) */ into DLMRT_OWNER.FIN_PLAN_FINANCE 
  
 WITH
        fact_q
        AS
            (SELECT /*+PARALLEL(8) inline no_merge
                      USE_HASH(FP_FACT PRDDIM PROJDIM AWDIM SPSRDIM PRI_SPONSOR)
                      */
                    FP_FACT.ACCOUNTING_PERIOD,
                    PRDDIM.FISCAL_YEAR,
                    PRDDIM.FISCAL_PERIOD,
                    PRDDIM.MONTH_NAME_SHORT,
                    FP_FACT.UM_HR_DEPTID,
                    FP_FACT.SOURCE,
                    FP_FACT.LEDGER_GROUP,
                    FP_FACT.CLASS_FLD,
                    FP_FACT.AF_JOURNAL_FLAG,
                    PROJDIM.PROJECT_ID,
                    PROJDIM.PROJECT_DESCR,
                    PROJDIM.GRANT_FLAG,
                    PROJDIM.PROJECT_STATUS,
                    AWDIM.AWARD_TYPE,
                    AWDIM.PRIMARY_SPONSOR_NAME,
                    PRI_SPONSOR.SPONSOR_TYPE    PRIMARY_SPONSOR_TYPE,
                    AWDIM.BILL_TO_SPONSOR_NAME,      -- Pass Thru Sponsor Name
                    SPSRDIM.SPONSOR_TYPE        BILL_TO_SPONSOR_TYPE, -- Pass Thru Sponsor Type
                    CASE
                        WHEN AWDIM.PRIMARY_SPONSOR_NAME IS NULL
                        THEN
                            AWDIM.BILL_TO_SPONSOR_NAME
                        ELSE
                            AWDIM.PRIMARY_SPONSOR_NAME
                    END                         COMBO_SPONSOR_NAME,
                    CASE
                        WHEN AWDIM.PRIMARY_SPONSOR_NAME IS NULL
                        THEN
                            SPSRDIM.SPONSOR_TYPE
                        ELSE
                            PRI_SPONSOR.SPONSOR_TYPE
                    END                         COMBO_SPONSOR_TYPE,
                    AWDIM.AWARD_ID_SHORT_TITLE,
                    FP_FACT.ACTUALS_AMOUNT      ACTUALS_AMOUNT,
                    --keys
                    FP_FACT.BU_KEY_ID,
                    FP_FACT.ORG_KEY_ID,
                    FP_FACT.FUND_KEY_ID,
                    FP_FACT.PROJECT_KEY_ID,
                    FP_FACT.ACCOUNT_KEY_ID,
                    FP_FACT.PROGRAM_KEY_ID
               FROM fsmrt_owner.ACTUALS_FP_FACT  FP_FACT
                    INNER JOIN fsmrt_owner.PERIOD2_DIM PRDDIM
                        ON (    PRDDIM.CALENDAR_DATE =
                                FP_FACT.PERIOD_END_DATE
                            AND PRDDIM.FISCAL_YEAR >= 2019)
                    INNER JOIN fsmrt_owner.PROJECT_PROJ_DIM_V PROJDIM
                        ON (FP_FACT.PROJECT_KEY_ID = PROJDIM.PROJECT_KEY_ID)
                    INNER JOIN fsmrt_owner.AWARD_CONTRACT_DIM AWDIM
                        ON (PROJDIM.AWARD_KEY_ID = AWDIM.AWARD_KEY_ID)
                    INNER JOIN fsmrt_owner.SPONSOR_DIM SPSRDIM
                        ON (AWDIM.SPONSOR_KEY_ID = SPSRDIM.SPONSOR_KEY_ID)
                    LEFT JOIN fsmrt_owner.SPONSOR_DIM PRI_SPONSOR
                        ON (AWDIM.PRIMARY_SPONSOR_ID = PRI_SPONSOR.SPONSOR_ID)),
        fnd_q
        AS
            (SELECT /*+ inline no_merge
                 USE_HASH(FUNDTREEDIM FUNDXREFTREE)*/
                    FUNDTREEDIM.FUND_KEY_ID,
                    CASE
                        WHEN FUNDTREEDIM.FUND_LEVEL_2 = 'AGENCY' THEN 'Y'
                        ELSE 'N'
                    END    AS AGENCY_FUND_FLAG
               FROM fsmrt_owner.FUND_TREE_DIM  FUNDTREEDIM,
                    fsmrt_owner.TREE_XREF_DIM  FUNDXREFTREE
              WHERE     FUNDTREEDIM.TREE_XREF_KEY_ID =
                        FUNDXREFTREE.TREE_XREF_KEY_ID
                    AND FUNDTREEDIM.CURRENT_FLAG = 'Y'
                    AND FUNDXREFTREE.SETID = 'UMASS'
                    AND FUNDXREFTREE.TREE_NAME = 'UM_FINST_FUND_GRP'),
        dtl_q
        AS
            (SELECT /*+  inline parallel(8)
           USE_HASH(ORGUNIT ORGUNITTREE BUSUNIT_TREE BUSUNIT PROGDIM FNDDIM FUNDTREEDIM  FUNDXREFTREE ACCDIM ACCTREEDIM TREEDIM)
                  */
                    BUSUNIT.BUSINESS_UNIT_PARENT     AS CAMPUS,
                    BUSUNIT.BUSINESS_UNIT,
                    ORGUNIT.DEPTID,
                    ORGUNIT.DEPT_DESCR,
                    ORGUNITTREE.ORG_LEVEL_2_DESCR,
                    ORGUNITTREE.ORG_LEVEL_3_DESCR,
                    ORGUNITTREE.ORG_LEVEL_4_DESCR,
                    ORGUNITTREE.ORG_LEVEL_5_DESCR,
                    PROGDIM.PROGRAM_CODE,
                    PROGDIM.NACUBO_CODE,
                    PROGDIM.NACUBO_DESCR,
                    FNDDIM.FUND_CODE,
                    FNDDIM.FUND_DESCR,
                    ACCDIM.ACCOUNT,
                    ACCDIM.ACCOUNT_DESCR,
                    ACCTREEDIM.ACCOUNT_LEVEL_2_DESCR,
                    ACCTREEDIM.ACCOUNT_LEVEL_3_DESCR,
                    ACCTREEDIM.ACCOUNT_LEVEL_4_DESCR,
                    ACCTREEDIM.ACCOUNT_LEVEL_5_DESCR,
                    FUNDTREEDIM.FUND_LEVEL_2_DESCR,
                    FUNDTREEDIM.FUND_LEVEL_3_DESCR,
                    FUNDTREEDIM.FUND_LEVEL_4_DESCR,
                    FUNDTREEDIM.FUND_LEVEL_5_DESCR,
                    BUSUNIT.BU_KEY_ID,
                    ORGUNIT.ORG_KEY_ID,
                    FNDDIM.FUND_KEY_ID,
                    ACCDIM.ACCOUNT_KEY_ID,
                    PROGRAM_KEY_ID
               FROM fsmrt_owner.ORGANIZATION_DIM       ORGUNIT,
                    fsmrt_owner.ORGANIZATION_TREE_DIM  ORGUNITTREE,
                    fsmrt_owner.TREE_XREF_DIM          BUSUNIT_TREE,
                    fsmrt_owner.BUSINESS_UNIT_DIM      BUSUNIT,
                    fsmrt_owner.PROGRAM_DIM            PROGDIM,
                    fsmrt_owner.FUND_DIM               FNDDIM,
                    fsmrt_owner.FUND_TREE_DIM          FUNDTREEDIM,
                    fsmrt_owner.TREE_XREF_DIM          FUNDXREFTREE,
                    fsmrt_owner.ACCOUNT_DIM            ACCDIM,
                    fsmrt_owner.ACCOUNT_TREE_DIM       ACCTREEDIM,
                    fsmrt_owner.TREE_XREF_DIM          TREEDIM
              WHERE     ORGUNIT.ORG_KEY_ID = ORGUNITTREE.ORG_KEY_ID
                    AND ORGUNITTREE.TREE_XREF_KEY_ID =
                        BUSUNIT_TREE.TREE_XREF_KEY_ID
                    AND ORGUNITTREE.CURRENT_FLAG = 'Y'
                    AND BUSUNIT.BUSINESS_UNIT_PARENT = BUSUNIT_TREE.SETID
                    AND BUSUNIT_TREE.TREE_NAME = 'RPT_DEPARTMENT'
                    AND FNDDIM.FUND_KEY_ID = FUNDTREEDIM.FUND_KEY_ID
                    AND FUNDTREEDIM.TREE_XREF_KEY_ID =
                        FUNDXREFTREE.TREE_XREF_KEY_ID
                    AND FUNDTREEDIM.CURRENT_FLAG = 'Y'
                    AND FUNDXREFTREE.SETID = 'UMASS'
                    AND FUNDXREFTREE.TREE_NAME = 'UM_FINST_FUND_REV'
                    AND ACCDIM.ACCOUNT_KEY_ID = ACCTREEDIM.ACCOUNT_KEY_ID
                    AND TREEDIM.TREE_XREF_KEY_ID =
                        ACCTREEDIM.TREE_XREF_KEY_ID
                    AND ACCTREEDIM.CURRENT_FLAG = 'Y'
                    AND ACCTREEDIM.ACCOUNT_LEVEL_2 IN ('600899', '700899')
                    AND TREEDIM.SETID = 'UMASS'
                    AND TREEDIM.TREE_NAME = 'RPT_ACCOUNT_AXIOM')
      SELECT /*+inline parallel(8)
               USE_HASH(fact_q dtl_q fnd_q)
                      */
             dtl_q.CAMPUS,
             dtl_q.BUSINESS_UNIT,
             fact_q.FISCAL_YEAR,
             fact_q.FISCAL_PERIOD,
             fact_q.MONTH_NAME_SHORT,
             fact_q.ACCOUNTING_PERIOD,
             fact_q.SOURCE,
             dtl_q.ACCOUNT,
             dtl_q.ACCOUNT_DESCR,
             dtl_q.ORG_LEVEL_2_DESCR,
             dtl_q.ORG_LEVEL_3_DESCR,
             dtl_q.ORG_LEVEL_4_DESCR,
             dtl_q.ORG_LEVEL_5_DESCR,
             dtl_q.DEPTID                    AS FINANCE_DEPTID,
             dtl_q.DEPT_DESCR                AS FINANCE_DEPT_DESCR,
             fact_q.UM_HR_DEPTID             AS HR_DEPT_ID,
             dtl_q.FUND_CODE,
             dtl_q.FUND_DESCR,
             dtl_q.PROGRAM_CODE,
             dtl_q.NACUBO_CODE,
             dtl_q.NACUBO_DESCR,
             fact_q.PROJECT_ID,
             fact_q.PROJECT_DESCR,
             fact_q.GRANT_FLAG,
             fact_q.LEDGER_GROUP,
             dtl_q.ACCOUNT_LEVEL_2_DESCR,
             dtl_q.ACCOUNT_LEVEL_3_DESCR,
             dtl_q.ACCOUNT_LEVEL_4_DESCR,
             dtl_q.ACCOUNT_LEVEL_5_DESCR,
             dtl_q.FUND_LEVEL_2_DESCR,
             dtl_q.FUND_LEVEL_3_DESCR,
             dtl_q.FUND_LEVEL_4_DESCR,
             dtl_q.FUND_LEVEL_5_DESCR,
             fact_q.CLASS_FLD,
             fact_q.PROJECT_STATUS,
             fact_q.AWARD_TYPE,
             fact_q.PRIMARY_SPONSOR_NAME,
             fact_q.PRIMARY_SPONSOR_TYPE,
             fact_q.BILL_TO_SPONSOR_NAME,
             fact_q.BILL_TO_SPONSOR_TYPE,
             fact_q.COMBO_SPONSOR_NAME,
             fact_q.COMBO_SPONSOR_TYPE,
             fact_q.AWARD_ID_SHORT_TITLE,
             fnd_q.AGENCY_FUND_FLAG,
             fact_q.BU_KEY_ID,
             fact_q.ACCOUNT_KEY_ID,
             fact_q.AF_JOURNAL_FLAG,
             fact_q.FUND_KEY_ID,
             fact_q.ORG_KEY_ID,
             fact_q.PROGRAM_KEY_ID,
             SUM (fact_q.ACTUALS_AMOUNT)     AS Actuals_amount,
             sysdate as INSERT_time
        FROM fact_q, dtl_q, fnd_q
       WHERE     fact_q.BU_KEY_ID = dtl_q.BU_KEY_ID
             AND dtl_q.PROGRAM_KEY_ID = fact_q.PROGRAM_KEY_ID
             AND dtl_q.ORG_KEY_ID = fact_q.ORG_KEY_ID
             AND fact_q.PROJECT_KEY_ID = fact_q.PROJECT_KEY_ID
             AND dtl_q.FUND_KEY_ID = fact_q.FUND_KEY_ID
             AND dtl_q.ACCOUNT_KEY_ID = fact_q.ACCOUNT_KEY_ID
             AND fact_q.FUND_KEY_ID = fnd_q.FUND_KEY_ID(+)
    GROUP BY dtl_q.CAMPUS,
             dtl_q.BUSINESS_UNIT,
             fact_q.FISCAL_YEAR,
             fact_q.FISCAL_PERIOD,
             fact_q.MONTH_NAME_SHORT,
             fact_q.ACCOUNTING_PERIOD,
             fact_q.SOURCE,
             dtl_q.ACCOUNT,
             dtl_q.ACCOUNT_DESCR,
             dtl_q.ORG_LEVEL_2_DESCR,
             dtl_q.ORG_LEVEL_3_DESCR,
             dtl_q.ORG_LEVEL_4_DESCR,
             dtl_q.ORG_LEVEL_5_DESCR,
             dtl_q.DEPTID,
             dtl_q.DEPT_DESCR,
             fact_q.UM_HR_DEPTID,
             dtl_q.FUND_CODE,
             dtl_q.FUND_DESCR,
             dtl_q.PROGRAM_CODE,
             dtl_q.NACUBO_CODE,
             dtl_q.NACUBO_DESCR,
             fact_q.PROJECT_ID,
             fact_q.PROJECT_DESCR,
             fact_q.GRANT_FLAG,
             fact_q.LEDGER_GROUP,
             dtl_q.ACCOUNT_LEVEL_2_DESCR,
             dtl_q.ACCOUNT_LEVEL_3_DESCR,
             dtl_q.ACCOUNT_LEVEL_4_DESCR,
             dtl_q.ACCOUNT_LEVEL_5_DESCR,
             dtl_q.FUND_LEVEL_2_DESCR,
             dtl_q.FUND_LEVEL_3_DESCR,
             dtl_q.FUND_LEVEL_4_DESCR,
             dtl_q.FUND_LEVEL_5_DESCR,
             fact_q.CLASS_FLD,
             fact_q.PROJECT_STATUS,
             fact_q.AWARD_TYPE,
             fact_q.PRIMARY_SPONSOR_NAME,
             fact_q.PRIMARY_SPONSOR_TYPE,
             fact_q.BILL_TO_SPONSOR_NAME,
             fact_q.BILL_TO_SPONSOR_TYPE,
             fact_q.COMBO_SPONSOR_NAME,
             fact_q.COMBO_SPONSOR_TYPE,
             fact_q.AWARD_ID_SHORT_TITLE,
             fnd_q.AGENCY_FUND_FLAG,
             fact_q.BU_KEY_ID,
             fact_q.ACCOUNT_KEY_ID,
             fact_q.AF_JOURNAL_FLAG,
             fact_q.FUND_KEY_ID,
             fact_q.ORG_KEY_ID,
             fact_q.PROGRAM_KEY_ID;
           
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of FIN_PLAN_FINANCE rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'FIN_PLAN_FINANCE',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table DLMRT_OWNER.FIN_PLAN_FINANCE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('DLMRT_OWNER','FIN_PLAN_FINANCE');

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

END FIN_PLAN_FINANCE_P;
/
