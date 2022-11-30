DROP PROCEDURE DLMRT_OWNER.FIN_PLAN_EMPL_FTE_P
/

--
-- FIN_PLAN_EMPL_FTE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE DLMRT_OWNER."FIN_PLAN_EMPL_FTE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--Created                    -- Smitha Paul
--Date                       -- 4/14/2022
--Loads table                -- FIN_PLAN_EMPL_FTE


------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'FIN_PLAN_EMPL_FTE';
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

strMessage01    := 'Truncating table DLMRT_OWNER.FIN_PLAN_EMPL_FTE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table DLMRT_OWNER.FIN_PLAN_EMPL_FTE';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table DLMRT_OWNER.FIN_PLAN_EMPL_FTE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('DLMRT_OWNER','FIN_PLAN_EMPL_FTE');


strMessage01    := 'Inserting data into DLMRT_OWNER.FIN_PLAN_EMPL_FTE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into DLMRT_OWNER.FIN_PLAN_EMPL_FTE';				
insert /*+ append enable_parallel_dml parallel(8) */ into DLMRT_OWNER.FIN_PLAN_EMPL_FTE 
 SELECT DISTINCT
             FISCAL_YEAR,
             BUSINESS_UNIT,
             LEVEL_1_CAT_DESCR,                      --restricted/unrestricted
             LEVEL_2_CAT_DESCR,                              --operating group
             LEVEL_3_CAT_DESCR,                          --faculty/non-faculty
             LEVEL_4_CAT_DESCR,                                         --EEO6
             TENURE_STATUS_DESCR,
             CASE
                 WHEN     tenure_status_descr = 'Non Tenure Track'
                      AND (UNION_CD = 'LTI' OR UNION_CD LIKE '%00')
                 THEN
                     'Temporary / Non-Continuing'
                 WHEN     tenure_status_descr = 'Non Tenure Track'
                      AND (   UNION_CD <> 'LTI' AND UNION_CD NOT LIKE '%00'
                           OR UNION_CD IS NULL)
                 THEN
                     'Permanent / Continuing'
             END          tenure_sub_status,
             DEPTID,
             DEPT_DESCR,                     --department depending on campus?
             ORG_LEVEL_1_DESCR,
             ORG_LEVEL_2_DESCR,                              --functional area
             ORG_LEVEL_3_DESCR,                             --college/division
             ORG_LEVEL_4_DESCR, --department or college/division depending on campus?
             ORG_LEVEL_5_DESCR,
             SUM (FTE)    FTE,
             FISCAL_PERIOD,
             FISCAL_YEAR_PERIOD_DESCR,
             sysdate as  insert_time
        FROM (  SELECT /*+PARALLEL(8) inline no_merge */
                       BUSHR.BUSINESS_UNIT,
                       DATED.FISCAL_YEAR,
                       DATED.FISCAL_PERIOD,
                       DATED.FISCAL_YEAR_PERIOD_DESCR,
                       --PERSON
                       PERS.PERSON_NM_ID,
                       PERS.PERSON_ID,
                       SUM (PAYR.ALLOCATION_PCT * EMPL1.EMPL_FTE_PCT)
                           FTE,
                       CASE
                           WHEN     PERS.TENURE_STATUS = '-'
                                AND JOBC.EEO6_CD <> '2'
                           THEN
                               NULL
                           WHEN PERS.TENURE_STATUS IN ('PRV', 'TEN')
                           THEN
                               'Tenured'
                           WHEN PERS.TENURE_STATUS IN ('EXT', 'NTK', 'TTR')
                           THEN
                               'Tenure Track'
                           ELSE
                               'Non Tenure Track'
                       END
                           TENURE_STATUS_DESCR,
                       --JOB
                       JOBC.JOBCODE_ID,
                       JOBC.JOBCODE_LD,
                       DEPT.DEPT_ID,                           --job's dept
                       DEPT.DEPT_LD,                           --job's dept
                       EMPL.EMPL_CLS_LD,
                       EMPL.EMPL_RCD,
                       EMPL.EMPL_STAT_CD,
                       HRACC.HR_ACCOUNT_CODE,
                       UNI.UNION_CD,
                       UNI.DESCR
                           UNION_CD_DESCR,
                       --FUNDING CATEGORY
                       PAYRATE.LEVEL_1_CAT_DESCR,
                       PAYRATE.LEVEL_2_CAT_DESCR,
                       PAYRATE.LEVEL_3_CAT_DESCR,
                       PAYRATE.LEVEL_4_CAT_DESCR,
                       --FINANCE ATTRS
                       ACCD.ACCOUNT,          --FDM Account for HR account
                       ORG1.DEPT_DESCR,  --FDM Organization for HR account
                       ORG1.DEPTID,      --FDM Organization for HR account
                       FND1.FUND_CODE,           --FDM Fund for HR account
                       FNDTREE.FUND_LEVEL_1_DESCR,
                       FNDTREE.FUND_LEVEL_2_DESCR,
                       FNDTREE.FUND_LEVEL_3_DESCR,
                       FNDTREE.FUND_LEVEL_4_DESCR,
                       ORGTREE.SETID,
                       ORGTREE.ORG_LEVEL_1_DESCR,
                       ORGTREE.ORG_LEVEL_2_DESCR,
                       ORGTREE.ORG_LEVEL_3_DESCR,
                       ORGTREE.ORG_LEVEL_4_DESCR,
                       ORGTREE.ORG_LEVEL_5_DESCR
                  FROM HRMRT_OWNER.D_AF_ATTRIBUTE_PAYRATE_VW PAYRATE, /* D_AF_ATTRIBUTE_PAYRATE */
                       FSMRT_OWNER.FUND_DIM               FND, /* FDM Fund */
                       FSMRT_OWNER.TREE_XREF_DIM          TREEX, /* FDM Tree Xref Fund */
                       FSMRT_OWNER.FUND_TREE_DIM          FNDTREE, /* FDM Fund Tree */
                       FSMRT_OWNER.TREE_XREF_DIM          TREEXD, /* FDM Tree Xref Organizaton */
                       FSMRT_OWNER.ORGANIZATION_TREE_DIM  ORGTREE, /* FDM Organization Tree */
                       FSMRT_OWNER.ORGANIZATION_DIM       ORG, /* FDM Organization */
                       HRMRT_OWNER.UM_D_HR_ACCOUNT        HRACC, /* D_HR_ACCOUNT */
                       FSMRT_OWNER.ACCOUNT_DIM            ACCD, /* FDM Account for HR account */
                       FSMRT_OWNER.BUSINESS_UNIT_DIM      BUS, /* FDM Business Unit */
                       FSMRT_OWNER.FUND_DIM               FND1, /* FDM Fund for HR account */
                       FSMRT_OWNER.ORGANIZATION_DIM       ORG1, /* FDM Organization for HR account */
                       HRMRT_OWNER.UM_D_EMPL_JOB_VW       EMPL, /* D_EMPL_JOB_Dim */
                       HRMRT_OWNER.UM_D_DEPT_VW           DEPT, /* D_DEPT */
                       HRMRT_OWNER.UM_D_UNION             UNI, /* D_UNION */
                       HRMRT_OWNER.UM_D_JOBCODE_VW        JOBC, /* D_JOBCODE */
                       HRMRT_OWNER.UM_D_PERSON_HR_VW      PERS, /* D_PERSON */
                       COMMON_OWNER.DATE_DIM              DATED, /* DATE_DIM_Pay_End_Date (Pay Facts) */
                       HRMRT_OWNER.UM_D_BUSINESS_UNIT_HR_VW BUSHR, /* D_BUSINESS_UNIT */
                       HRMRT_OWNER.UM_D_EMPL_JOB_VW       EMPL1, /* D_EMPL_JOB_Pay_Rate */
                       HRMRT_OWNER.UM_F_PAY_RATE          PAYR /* F_PAY_RATE */
                 WHERE     PAYR.PAY_RATE_SID = PAYRATE.PAY_RATE_SID
                       AND PAYR.FUND_KEY_ID = FND.FUND_KEY_ID
                       AND FND.FUND_KEY_ID = FNDTREE.FUND_KEY_ID
                       AND PAYR.BU_KEY_ID = BUS.BU_KEY_ID
                       AND EMPL.EMPL_JOB_SID = EMPL1.EMPL_JOB_SID
                       AND DEPT.DEPT_SID = EMPL1.DEPT_SID
                       AND UNI.UNION_SID = EMPL1.UNION_SID
                       AND JOBC.JOBCODE_SID = EMPL1.JOBCODE_SID
                       AND PERS.PERSON_SID = EMPL1.PERSON_SID
                       AND DATED.CALENDAR_DATE = PAYR.PAY_END_DT
                       AND BUSHR.BU_SID = EMPL1.BU_SID
                       AND PAYR.EMPL_JOB_SID = EMPL1.EMPL_JOB_SID
                       AND DATED.FISCAL_PERIOD = 3
                       AND DATED.FISCAL_YEAR >= 2018
                       AND DATED.LAST_PAY_END_MONTH_FLAG = 'Y'
                       AND PAYRATE.IN_OUT_CD = 'IN'
                       AND TREEX.TREE_NAME = 'UM_FINST_FUND_NET'
                       AND TREEX.TREE_XREF_KEY_ID = FNDTREE.TREE_XREF_KEY_ID
                       AND HRACC.ACCOUNT_KEY_ID = ACCD.ACCOUNT_KEY_ID
                       AND HRACC.FUND_KEY_ID = FND1.FUND_KEY_ID
                       AND HRACC.ORG_KEY_ID = ORG1.ORG_KEY_ID
                       AND HRACC.BU_KEY_ID = BUS.BU_KEY_ID
                       AND HRACC.HR_ACCOUNT_SID = PAYR.HR_ACCT_CD_SID
                       AND FNDTREE.CURRENT_FLAG = 'Y'
                       AND TREEXD.TREE_XREF_KEY_ID = ORGTREE.TREE_XREF_KEY_ID
                       AND ORG.ORG_KEY_ID = ORGTREE.ORG_KEY_ID
                       AND PAYR.ORG_KEY_ID = ORG.ORG_KEY_ID
                       AND TREEXD.TREE_NAME = 'RPT_DEPARTMENT'
                       AND ORGTREE.CURRENT_FLAG = 'Y'
                       AND NOT (    EMPL.ACN_CD IN ('LOA', 'PLA')
                                AND EMPL.ACN_RSN_CD = 'FUR')
              GROUP BY BUSHR.BUSINESS_UNIT,
                       DEPT.DEPT_ID,
                       DEPT.DEPT_LD,
                       JOBC.JOBCODE_ID,
                       JOBC.JOBCODE_LD,
                       PERS.PERSON_ID,
                       PERS.TENURE_STATUS,
                       PERS.PERSON_NM_ID,
                       DATED.FISCAL_YEAR_PERIOD_DESCR,
                       DATED.FISCAL_PERIOD,
                       DATED.CALENDAR_DATE_YYYYMMDD,
                       HRACC.HR_ACCOUNT_CODE,
                       PAYR.ALLOCATION_PCT,
                       PAYR.ANNUAL_ALLOCATION,
                       UNI.DESCR,
                       UNI.UNION_CD,
                       EMPL.EMPL_RCD,
                       EMPL.EMPL_CLS_LD,
                       EMPL.EMPL_STAT_CD,
                       ACCD.ACCOUNT,
                       FND1.FUND_CODE,
                       ORG1.DEPTID,
                       ORG1.DEPT_DESCR,
                       DATED.FISCAL_YEAR,
                       FNDTREE.FUND_CODE,
                       FNDTREE.FUND_LEVEL_1_DESCR,
                       FNDTREE.FUND_LEVEL_2_DESCR,
                       FNDTREE.FUND_LEVEL_3_DESCR,
                       FNDTREE.FUND_LEVEL_4_DESCR,
                       ORGTREE.SETID,
                       ORGTREE.ORG_LEVEL_1_DESCR,
                       ORGTREE.ORG_LEVEL_2_DESCR,
                       ORGTREE.ORG_LEVEL_3_DESCR,
                       ORGTREE.ORG_LEVEL_4_DESCR,
                       ORGTREE.ORG_LEVEL_5_DESCR,
                       PAYRATE.LEVEL_1_CAT_DESCR,
                       PAYRATE.LEVEL_2_CAT_DESCR,
                       PAYRATE.LEVEL_3_CAT_DESCR,
                       PAYRATE.LEVEL_4_CAT_DESCR,
                       JOBC.EEO6_CD)
    GROUP BY FISCAL_YEAR,
             FISCAL_PERIOD,
             FISCAL_YEAR_PERIOD_DESCR,
             BUSINESS_UNIT,
             LEVEL_1_CAT_DESCR,
             LEVEL_2_CAT_DESCR,
             LEVEL_3_CAT_DESCR,
             LEVEL_4_CAT_DESCR,
             TENURE_STATUS_DESCR,
             CASE
                 WHEN     tenure_status_descr = 'Non Tenure Track'
                      AND (UNION_CD = 'LTI' OR UNION_CD LIKE '%00')
                 THEN
                     'Temporary / Non-Continuing'
                 WHEN     tenure_status_descr = 'Non Tenure Track'
                      AND (   UNION_CD <> 'LTI' AND UNION_CD NOT LIKE '%00'
                           OR UNION_CD IS NULL)
                 THEN
                     'Permanent / Continuing'
             END,
             SETID,
             ORG_LEVEL_1_DESCR,
             ORG_LEVEL_2_DESCR,
             ORG_LEVEL_3_DESCR,
             ORG_LEVEL_4_DESCR,
             ORG_LEVEL_5_DESCR,
             DEPTID,
             DEPT_DESCR;



           
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of FIN_PLAN_EMPL_FTE rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'FIN_PLAN_EMPL_FTE',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table DLMRT_OWNER.FIN_PLAN_EMPL_FTE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('DLMRT_OWNER','FIN_PLAN_EMPL_FTE');

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

END FIN_PLAN_EMPL_FTE_P;
/
