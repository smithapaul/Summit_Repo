DROP PROCEDURE DLMRT_OWNER.FIN_PLAN_PAYROLL_P
/

--
-- FIN_PLAN_PAYROLL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."FIN_PLAN_PAYROLL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--Created                    -- Smitha Paul
--Date                       -- 3/31/2022
--Loads table                -- FIN_PLAN_PAYROLL


------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'FIN_PLAN_PAYROLL';
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

strMessage01    := 'Truncating table DLMRT_OWNER.FIN_PLAN_PAYROLL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table DLMRT_OWNER.FIN_PLAN_PAYROLL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table DLMRT_OWNER.FIN_PLAN_PAYROLL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('DLMRT_OWNER','FIN_PLAN_PAYROLL');


strMessage01    := 'Inserting data into DLMRT_OWNER.FIN_PLAN_PAYROLL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into DLMRT_OWNER.FIN_PLAN_PAYROLL';				
insert /*+ append enable_parallel_dml parallel(8) */ into DLMRT_OWNER.FIN_PLAN_PAYROLL 
  
  WITH
        fact_q
        AS
            (SELECT /*+PARALLEL(8) inline no_merge
                  USE_HASH(FP_FACT PRDDIM JOBVW BUSUNIT PROJDIM AWDIM SPSRDIM PRI_SPONSOR EMPDIM PERDIM)
                  full(FP_FACT)
                  */
                    PRDDIM.FISCAL_YEAR,
                    FP_FACT.EMPL_RCD,
                    FP_FACT.UM_HR_DEPTID,
                    CONCAT (FP_FACT.SETID_JOBCODE, FP_FACT.JOBCODE)
                        AS JOBCODE_CAMPUS,
                    FP_FACT.UNION_CD,
                    FP_FACT.UM_MARS_OBJECT_CODE,
                    FP_FACT.EARNINGS_CODE,
                    FP_FACT.SOURCE,
                    FP_FACT.ACTUALS_AMOUNT,
                    PROJDIM.PROJECT_DESCR,
                    PROJDIM.PROJECT_ID,
                    PROJDIM.PROJECT_STATUS,
                    PROJDIM.GRANT_FLAG,
                    AWDIM.AWARD_TYPE,
                    AWDIM.PRIMARY_SPONSOR_NAME,            --PASS THRU SPONSOR
                    PRI_SPONSOR.SPONSOR_TYPE
                        PRIMARY_SPONSOR_TYPE,
                    AWDIM.BILL_TO_SPONSOR_NAME,
                    SPSRDIM.SPONSOR_TYPE
                        BILL_TO_SPONSOR_TYPE,
                    CASE
                        WHEN AWDIM.PRIMARY_SPONSOR_NAME IS NULL
                        THEN
                            AWDIM.BILL_TO_SPONSOR_NAME
                        ELSE
                            AWDIM.PRIMARY_SPONSOR_NAME
                    END
                        COMBO_SPONSOR_NAME,
                    CASE
                        WHEN AWDIM.PRIMARY_SPONSOR_NAME IS NULL
                        THEN
                            SPSRDIM.SPONSOR_TYPE
                        ELSE
                            PRI_SPONSOR.SPONSOR_TYPE
                    END
                        COMBO_SPONSOR_TYPE,
                    AWDIM.AWARD_ID_SHORT_TITLE,
                    BUSUNIT.BUSINESS_UNIT,
                    JOBVW.JOBCODE_ID,
                    JOBVW.EEO6_CD,
                    JOBVW.EEO6_SD,
                    JOBVW.EEO6_LD,
                    JOBVW.SETID,
                    EMPDIM.EMPLOYEE_KEY_ID,
                    EMPDIM.EMPLOYEE_ID,
                    CONCAT (EMPDIM.EMPLOYEE_ID,
                            CAST (EMPDIM.EMPLOYEE_RECORD AS VARCHAR (2)))
                        UNIQUE_ID,
                    EMPDIM.EMPLOYEE_NAME,
                    EMPDIM.EMPL_CLS_CD,
                    EMPDIM.EMPL_CLS_LD,
                    EMPDIM.JOB_TITLE,
                    EMPDIM.EMPLOYEE_TYPE,
                    EMPDIM.EMPLOYEE_RECORD_STATUS,
                    EMPDIM.WORKGROUP,
                    EMPDIM.WORKGROUP_DESCR,
                    PERDIM.TENURE_STATUS,
                    CASE
                        WHEN     PERDIM.TENURE_STATUS = '-'
                             AND JOBVW.EEO6_CD <> '2'
                        THEN
                            NULL
                        WHEN PERDIM.TENURE_STATUS IN ('PRV', 'TEN')
                        THEN
                            'Tenure'
                        WHEN PERDIM.TENURE_STATUS IN ('EXT', 'NTK', 'TTR')
                        THEN
                            'Tenure Track'
                        ELSE
                            'Non Tenure Track'
                    END
                        AS TENURE_STATUS_DESCR,
                    --keys
                    FP_FACT.JOBCODE,
                    FP_FACT.SETID_JOBCODE,
                    FP_FACT.BU_KEY_ID,
                    FP_FACT.ORG_KEY_ID,
                    --FP_FACT.EMPLOYEE_KEY_ID,
                    FP_FACT.FUND_KEY_ID,
                    FP_FACT.PROJECT_KEY_ID,
                    FP_FACT.ACCOUNT_KEY_ID
               FROM fsmrt_owner.ACTUALS_FP_FACT  FP_FACT
                    INNER JOIN fsmrt_owner.PERIOD2_DIM PRDDIM
                        ON (PRDDIM.CALENDAR_DATE = FP_FACT.PERIOD_END_DATE)
                    INNER JOIN fsmrt_owner.BUSINESS_UNIT_DIM BUSUNIT
                        ON (FP_FACT.BU_KEY_ID = BUSUNIT.BU_KEY_ID)
                    INNER JOIN hrmrt_owner.UM_D_JOBCODE_VW JOBVW
                        ON (    FP_FACT.JOBCODE = JOBVW.JOBCODE_ID
                            AND FP_FACT.SETID_JOBCODE = JOBVW.SETID
                            AND JOBVW.CURRENT_IND = 'Y')
                    INNER JOIN fsmrt_owner.PROJECT_PROJ_DIM_V PROJDIM
                        ON (FP_FACT.PROJECT_KEY_ID = PROJDIM.PROJECT_KEY_ID)
                    INNER JOIN fsmrt_owner.AWARD_CONTRACT_DIM AWDIM
                        ON (PROJDIM.AWARD_KEY_ID = AWDIM.AWARD_KEY_ID)
                    INNER JOIN fsmrt_owner.SPONSOR_DIM SPSRDIM
                        ON (AWDIM.SPONSOR_KEY_ID = SPSRDIM.SPONSOR_KEY_ID)
                    INNER JOIN fsmrt_owner.EMPLOYEE_DIM EMPDIM
                        ON (EMPDIM.EMPLOYEE_KEY_ID = FP_FACT.EMPLOYEE_KEY_ID)
                    INNER JOIN HRMRT_OWNER.PS_D_PERSON PERDIM
                        ON (PERDIM.PERSON_ID = EMPDIM.EMPLOYEE_ID)
                    LEFT JOIN fsmrt_owner.SPONSOR_DIM PRI_SPONSOR
                        ON (AWDIM.PRIMARY_SPONSOR_ID = PRI_SPONSOR.SPONSOR_ID)
              WHERE PRDDIM.FISCAL_YEAR >= 2017 AND FP_FACT.SOURCE = 'PAY'),
        fnd_q
        AS
            (SELECT /*+ PARALLEL(8) inline
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
        org_q
        AS
            (SELECT /*+ PARALLEL(8) inline
                 USE_HASH(ORGUNIT ORGUNITTREE BUSUNIT_TREE)
                  */
                    ORGUNIT.ORG_KEY_ID,
                    ORGUNIT.DEPTID,
                    ORGUNIT.DEPT_DESCR,
                    ORGUNITTREE.ORG_LEVEL_2_DESCR,
                    ORGUNITTREE.ORG_LEVEL_3_DESCR,
                    ORGUNITTREE.ORG_LEVEL_4_DESCR,
                    ORGUNITTREE.ORG_LEVEL_5_DESCR
               FROM fsmrt_owner.ORGANIZATION_DIM  ORGUNIT
                    INNER JOIN fsmrt_owner.ORGANIZATION_TREE_DIM ORGUNITTREE
                        ON (ORGUNIT.ORG_KEY_ID = ORGUNITTREE.ORG_KEY_ID)
                    INNER JOIN fsmrt_owner.TREE_XREF_DIM BUSUNIT_TREE
                        ON (ORGUNITTREE.TREE_XREF_KEY_ID =
                            BUSUNIT_TREE.TREE_XREF_KEY_ID)
              WHERE     ORGUNIT.BUSINESS_UNIT_PARENT IN ('N/A',
                                                         'UMAMH',
                                                         'UMASS',
                                                         'UMBOS',
                                                         'UMCEN',
                                                         'UMDAR',
                                                         'UMFND',
                                                         'UMLOW',
                                                         'UMWOR')
                    AND BUSUNIT_TREE.TREE_NAME = 'RPT_DEPARTMENT'
                    AND ORGUNITTREE.CURRENT_FLAG = 'Y'),
        acc_q
        AS
            (SELECT /*+ PARALLEL(8) inline
                 USE_HASH(ACCDIM ACCTREEDIM TREEDIM)*/
                    ACCDIM.ACCOUNT_KEY_ID,
                    ACCDIM.ACCOUNT,
                    ACCDIM.ACCOUNT_DESCR,
                    ACCTREEDIM.ACCOUNT_LEVEL_2,
                    ACCTREEDIM.ACCOUNT_LEVEL_2_DESCR,
                    ACCTREEDIM.ACCOUNT_LEVEL_3,
                    ACCTREEDIM.ACCOUNT_LEVEL_3_DESCR,
                    ACCTREEDIM.ACCOUNT_LEVEL_4,
                    ACCTREEDIM.ACCOUNT_LEVEL_4_DESCR,
                    ACCTREEDIM.ACCOUNT_LEVEL_5,
                    ACCTREEDIM.ACCOUNT_LEVEL_5_DESCR
               FROM fsmrt_owner.ACCOUNT_DIM  ACCDIM
                    INNER JOIN fsmrt_owner.ACCOUNT_TREE_DIM ACCTREEDIM
                        ON (ACCDIM.ACCOUNT_KEY_ID = ACCTREEDIM.ACCOUNT_KEY_ID)
                    INNER JOIN fsmrt_owner.TREE_XREF_DIM TREEDIM
                        ON     (TREEDIM.TREE_XREF_KEY_ID =
                                ACCTREEDIM.TREE_XREF_KEY_ID)
                           AND ACCTREEDIM.CURRENT_FLAG = 'Y'
                           AND TREEDIM.TREE_NAME = 'RPT_ACCOUNT_AXIOM'
                           AND (ACCTREEDIM.ACCOUNT_LEVEL_2 IN
                                    ('700899', 'ALL_EXPENSES', 'EXPENSES'))),
        dtl_q
        AS
            (SELECT /*+ PARALLEL(8) inline
                USE_HASH(FNDDIM FUNDTREEDIM  FUNDXREFTREE)
                             */
                    FNDDIM.FUND_CODE,
                    FNDDIM.FUND_DESCR,
                    FUNDTREEDIM.FUND_LEVEL_2_DESCR,
                    FUNDTREEDIM.FUND_LEVEL_3_DESCR,
                    FUNDTREEDIM.FUND_LEVEL_4_DESCR,
                    FUNDTREEDIM.FUND_LEVEL_5_DESCR,
                    FNDDIM.FUND_KEY_ID
               FROM fsmrt_owner.FUND_DIM       FNDDIM,
                    fsmrt_owner.FUND_TREE_DIM  FUNDTREEDIM,
                    fsmrt_owner.TREE_XREF_DIM  FUNDXREFTREE
              WHERE     FNDDIM.FUND_KEY_ID = FUNDTREEDIM.FUND_KEY_ID
                    AND FUNDTREEDIM.TREE_XREF_KEY_ID =
                        FUNDXREFTREE.TREE_XREF_KEY_ID
                    AND FUNDTREEDIM.CURRENT_FLAG = 'Y'
                    AND FUNDXREFTREE.SETID = 'UMASS'
                    AND FUNDXREFTREE.TREE_NAME = 'UM_FINST_FUND_REV')
      SELECT /*+ PARALLEL(8) inline
           USE_HASH(fact_q fnd_q  org_q acc_q dtl_q)
                                */
             fact_q.FISCAL_YEAR,
             fact_q.BUSINESS_UNIT,
             fact_q.EMPLOYEE_ID,
             fact_q.EMPL_RCD,
             fact_q.UNIQUE_ID,
             fact_q.EMPLOYEE_NAME,
             fact_q.EMPL_CLS_CD,
             fact_q.EMPL_CLS_LD,
             org_q.DEPTID,
             org_q.DEPT_DESCR,
             org_q.ORG_LEVEL_2_DESCR,
             org_q.ORG_LEVEL_3_DESCR,
             org_q.ORG_LEVEL_4_DESCR,
             org_q.ORG_LEVEL_5_DESCR,
             fact_q.UM_HR_DEPTID,
             ' '                            AS HR_DEPT_DESCR,
             fact_q.JOBCODE_ID,
             fact_q.JOBCODE_CAMPUS,
             fact_q.JOB_TITLE,
             fact_q.UNION_CD,
             fact_q.UM_MARS_OBJECT_CODE,
             fact_q.EARNINGS_CODE,
             ' '                            EARN_CD_DESCR,
             fact_q.EMPLOYEE_TYPE,
             fact_q.EMPLOYEE_RECORD_STATUS,
             dtl_q.FUND_CODE,
             dtl_q.FUND_DESCR,
             dtl_q.FUND_LEVEL_2_DESCR,
             dtl_q.FUND_LEVEL_3_DESCR,
             dtl_q.FUND_LEVEL_4_DESCR,
             dtl_q.FUND_LEVEL_5_DESCR,
             fact_q.SOURCE,
             acc_q.ACCOUNT,
             acc_q.ACCOUNT_DESCR,
             acc_q.ACCOUNT_LEVEL_2,
             acc_q.ACCOUNT_LEVEL_2_DESCR,
             acc_q.ACCOUNT_LEVEL_3,
             acc_q.ACCOUNT_LEVEL_3_DESCR,
             acc_q.ACCOUNT_LEVEL_4,
             acc_q.ACCOUNT_LEVEL_4_DESCR,
             acc_q.ACCOUNT_LEVEL_5,
             acc_q.ACCOUNT_LEVEL_5_DESCR,
             fact_q.PROJECT_DESCR,
             fact_q.PROJECT_ID,
             fact_q.PROJECT_STATUS,
             fact_q.GRANT_FLAG,
             fact_q.AWARD_TYPE,
             fact_q.PRIMARY_SPONSOR_NAME,
             fact_q.PRIMARY_SPONSOR_TYPE,
             fact_q.BILL_TO_SPONSOR_NAME,
             fact_q.BILL_TO_SPONSOR_TYPE,
             fact_q.COMBO_SPONSOR_NAME,
             fact_q.COMBO_SPONSOR_TYPE,
             fact_q.AWARD_ID_SHORT_TITLE,
             fact_q.EEO6_CD,
             fact_q.EEO6_SD,
             fact_q.EEO6_LD,
             fact_q.WORKGROUP,
             fact_q.WORKGROUP_DESCR,
             fact_q.TENURE_STATUS,
             fact_q.TENURE_STATUS_DESCR,
             CASE
                 WHEN     fact_q.TENURE_STATUS_DESCR = 'Non Tenure Track'
                      AND (   fact_q.UNION_CD = 'LTI'
                           OR fact_q.UNION_CD LIKE '%00')
                 THEN
                     'Temporary / Non-Continuing'
                 WHEN     fact_q.TENURE_STATUS_DESCR = 'Non Tenure Track'
                      AND (       fact_q.UNION_CD <> 'LTI'
                              AND fact_q.UNION_CD NOT LIKE '%00'
                           OR fact_q.UNION_CD IS NULL)
                 THEN
                     'Permanent / Continuing'
             END                            TENURE_SUB_STATUS,
             fnd_q.AGENCY_FUND_FLAG,
             SUM (fact_q.ACTUALS_AMOUNT)    AS ACTUALS_AMOUNT,
             sysdate as insert_time
        FROM fact_q,
             dtl_q,
             --emp_q,
             fnd_q,
             org_q,
             acc_q
       WHERE              --fact_q.EMPLOYEE_KEY_ID = emp_q.EMPLOYEE_KEY_ID AND
                 fact_q.ORG_KEY_ID = org_q.ORG_KEY_ID
             AND fact_q.FUND_KEY_ID = dtl_q.FUND_KEY_ID
             AND fact_q.ACCOUNT_KEY_ID = acc_q.ACCOUNT_KEY_ID
             AND fact_q.FUND_KEY_ID = fnd_q.FUND_KEY_ID(+)
    GROUP BY fact_q.FISCAL_YEAR,
             fact_q.BUSINESS_UNIT,
             fact_q.EMPLOYEE_ID,
             fact_q.EMPL_RCD,
             fact_q.UNIQUE_ID,
             fact_q.EMPLOYEE_NAME,
             fact_q.EMPL_CLS_CD,
             fact_q.EMPL_CLS_LD,
             org_q.DEPTID,
             org_q.DEPT_DESCR,
             org_q.ORG_LEVEL_2_DESCR,
             org_q.ORG_LEVEL_3_DESCR,
             org_q.ORG_LEVEL_4_DESCR,
             org_q.ORG_LEVEL_5_DESCR,
             fact_q.UM_HR_DEPTID,
             fact_q.JOBCODE_ID,
             fact_q.JOBCODE_CAMPUS,
             fact_q.JOB_TITLE,
             fact_q.UNION_CD,
             fact_q.UM_MARS_OBJECT_CODE,
             fact_q.EARNINGS_CODE,
             fact_q.EMPLOYEE_TYPE,
             fact_q.EMPLOYEE_RECORD_STATUS,
             dtl_q.FUND_CODE,
             dtl_q.FUND_DESCR,
             dtl_q.FUND_LEVEL_2_DESCR,
             dtl_q.FUND_LEVEL_3_DESCR,
             dtl_q.FUND_LEVEL_4_DESCR,
             dtl_q.FUND_LEVEL_5_DESCR,
             fact_q.SOURCE,
             acc_q.ACCOUNT,
             acc_q.ACCOUNT_DESCR,
             acc_q.ACCOUNT_LEVEL_2,
             acc_q.ACCOUNT_LEVEL_2_DESCR,
             acc_q.ACCOUNT_LEVEL_3,
             acc_q.ACCOUNT_LEVEL_3_DESCR,
             acc_q.ACCOUNT_LEVEL_4,
             acc_q.ACCOUNT_LEVEL_4_DESCR,
             acc_q.ACCOUNT_LEVEL_5,
             acc_q.ACCOUNT_LEVEL_5_DESCR,
             fact_q.PROJECT_DESCR,
             fact_q.PROJECT_ID,
             fact_q.PROJECT_STATUS,
             fact_q.GRANT_FLAG,
             fact_q.AWARD_TYPE,
             fact_q.PRIMARY_SPONSOR_NAME,
             fact_q.PRIMARY_SPONSOR_TYPE,
             fact_q.BILL_TO_SPONSOR_NAME,
             fact_q.BILL_TO_SPONSOR_TYPE,
             fact_q.COMBO_SPONSOR_NAME,
             fact_q.COMBO_SPONSOR_TYPE,
             fact_q.AWARD_ID_SHORT_TITLE,
             fact_q.EEO6_CD,
             fact_q.EEO6_SD,
             fact_q.EEO6_LD,
             fact_q.WORKGROUP,
             fact_q.WORKGROUP_DESCR,
             fact_q.TENURE_STATUS,
             fact_q.TENURE_STATUS_DESCR,
             fnd_q.AGENCY_FUND_FLAG;
           
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of FIN_PLAN_PAYROLL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'FIN_PLAN_PAYROLL',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table DLMRT_OWNER.FIN_PLAN_PAYROLL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('DLMRT_OWNER','FIN_PLAN_PAYROLL');

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

END FIN_PLAN_PAYROLL_P;
/
