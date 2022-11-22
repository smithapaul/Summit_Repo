DROP PROCEDURE CSMRT_OWNER.UM_D_PERSON_SRVC_IND_P
/

--
-- UM_D_PERSON_SRVC_IND_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_PERSON_SRVC_IND_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--OLD tables               -- UM_R_SRVC_IND / UM_R_PERSON_SRVC_IND_VW, UM_D_PRSPCT_CAR_VW
--Loads target table       -- UM_D_PERSON_SRVC_IND
--UM_D_PERSON_SRVC_IND     -- Dependent on PS_D_PERSON
-- V01 4/22/2018           -- srikanth ,pabbu converted to proc from sql
-- V02 3/21/2019           -- doucette ,james converted proc to destructive load.
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_PERSON_SRVC_IND';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_PERSON_SRVC_IND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_PERSON_SRVC_IND');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_SRVC_IND disable constraint PK_UM_D_PERSON_SRVC_IND';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_PERSON_SRVC_IND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_PERSON_SRVC_IND';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_PERSON_SRVC_IND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_PERSON_SRVC_IND';				
insert into CSMRT_OWNER.UM_D_PERSON_SRVC_IND
with Q1 as ( 
select /*+ inline */ 
       STRM TERM_CD, SRC_SYS_ID, 
       DESCR, DESCRSHORT 
  from CSSTG_OWNER.PS_TERM_VAL_TBL
 where DATA_ORIGIN <> 'D'),
       Q2 as ( 
select /*+ inline */ 
       INSTITUTION INSTITUTION_CD, SRVC_IND_CD, SRC_SYS_ID, EFFDT,  
       SERVICE_IMPACT, TERM_CATEGORY, 
       dense_rank() over (partition by INSTITUTION, SRVC_IND_CD, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_SERVICE_IMPACT
 where DATA_ORIGIN <> 'D'),
       Q3 as (  
select /*+ inline */
       INSTITUTION INSTITUTION_CD, SERVICE_IMPACT, SRC_SYS_ID, EFFDT, EFF_STATUS, 
       DESCRSHORT SERVICE_IMPACT_SD, DESCR SERVICE_IMPACT_LD, 
       SCC_IMPACT_TERM SCC_IMPACT_TERM_FLG, SCC_IMPACT_DATE SCC_IMPACT_DATE_FLG, 
       POS_SRVC_IMPACT POS_SRVC_IMPACT_FLG, SYSTEM_FUNCTION SYSTEM_FUNCTION_FLG, 
       row_number() over (partition by INSTITUTION, SERVICE_IMPACT, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_SRVC_IMPACT_TBL 
 where DATA_ORIGIN <> 'D'),
       Q4 as (  
select /*+ inline */
       Q2.INSTITUTION_CD, Q2.SRVC_IND_CD, Q2.SRC_SYS_ID, 
       Q2.SERVICE_IMPACT, Q2.EFFDT, 
       Q3.SERVICE_IMPACT_SD, Q3.SERVICE_IMPACT_LD, 
       Q3.SCC_IMPACT_TERM_FLG, Q3.SCC_IMPACT_DATE_FLG, 
       Q3.POS_SRVC_IMPACT_FLG, Q3.SYSTEM_FUNCTION_FLG, 
       Q2.TERM_CATEGORY 
  from Q2
  join Q3
    on Q2.INSTITUTION_CD = Q3.INSTITUTION_CD 
   and Q2.SERVICE_IMPACT = Q3.SERVICE_IMPACT 
   and Q2.SRC_SYS_ID = Q3.SRC_SYS_ID
   and Q3.Q_ORDER = 1  
  where Q2.Q_ORDER = 1),
       Q5 as (  
select /*+ inline */
       INSTITUTION INSTITUTION_CD, SRVC_IND_CD, SRC_SYS_ID, EFFDT, EFF_STATUS, 
       DESCRSHORT SRVC_IND_SD, DESCR SRVC_IND_LD, 
       POS_SRVC_INDICATOR POS_SRVC_IND_FLG, SCC_HOLD_DISPLAY SCC_HOLD_DISP_FLG, 
       SCC_SI_PERS SCC_SI_PERS_FLG, SCC_SI_ORG SCC_SI_ORG_FLG, SCC_DFLT_ACTDATE SCC_DFLT_ACTDATE_FLG, 
       SCC_DFLT_ACTTERM SCC_DFLT_ACTTERM_FLG, DFLT_SRVC_IND_RSN, SRV_IND_DCSD_FLAG SRV_IND_DCSD_FLG, 
       row_number() over (partition by INSTITUTION, SRVC_IND_CD, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_SRVC_IND_CD_TBL
 where DATA_ORIGIN <> 'D'),
       Q6 as (  
select /*+ inline */
       INSTITUTION INSTITUTION_CD, SRVC_IND_CD, SRVC_IND_REASON, SRC_SYS_ID, EFFDT, 
       DESCRSHORT SRVC_IND_REASON_SD, DESCR SRVC_IND_REASON_LD, 
       DATA_ORIGIN,
       row_number() over (partition by INSTITUTION, SRVC_IND_CD, SRVC_IND_REASON, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_SRVC_IN_RSN_TBL
 where DATA_ORIGIN <> 'D'),
       S as (
select IND.INSTITUTION INSTITUTION_CD,
       IND.EMPLID PERSON_ID,
       IND.SRVC_IND_DTTM, 
       IND.SRC_SYS_ID,
--       Q5.EFFDT,  
       nvl(Q4.EFFDT,to_date('01-JAN-1900')) EFFDT,  
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID, 
       nvl(P.PERSON_SID, 2147483646) PERSON_SID, 
       IND.AMOUNT,
       IND.CONTACT,
       IND.CONTACT_ID,
       nvl(Q5.DFLT_SRVC_IND_RSN,'-') DFLT_SRVC_IND_RSN,
       IND.OPRID,
       IND.PLACED_METHOD,
       IND.PLACED_PERSON,
       IND.PLACED_PERSON_ID,
       IND.PLACED_PROCESS,
       nvl(Q4.POS_SRVC_IMPACT_FLG,'-') POS_SRVC_IMPACT_FLG, 
       nvl(Q5.POS_SRVC_IND_FLG,'-') POS_SRVC_IND_FLG,
       IND.POS_SRVC_INDICATOR,
       nvl(Q5.SCC_DFLT_ACTDATE_FLG,'-') SCC_DFLT_ACTDATE_FLG,
       nvl(Q5.SCC_DFLT_ACTTERM_FLG,'-') SCC_DFLT_ACTTERM_FLG,
       nvl(Q5.SCC_HOLD_DISP_FLG,'-') SCC_HOLD_DISP_FLG,
       nvl(Q4.SCC_IMPACT_DATE_FLG,'-') SCC_IMPACT_DATE_FLG, 
       nvl(Q4.SCC_IMPACT_TERM_FLG,'-') SCC_IMPACT_TERM_FLG, 
       IND.SCC_SI_END_TERM,
       IND.SCC_SI_END_DT, 
       nvl(ST.DESCRSHORT, '-') SCC_SI_END_TERM_SDESC,
       nvl(ST.DESCR, '-') SCC_SI_END_TERM_DESC,
       nvl(Q5.SCC_SI_ORG_FLG,'-') SCC_SI_ORG_FLG,
       nvl(Q5.SCC_SI_PERS_FLG,'-') SCC_SI_PERS_FLG,
       nvl(Q4.SERVICE_IMPACT,'-') SERVICE_IMPACT,
       nvl(Q4.SERVICE_IMPACT_SD,'-') SERVICE_IMPACT_SD,
       nvl(Q4.SERVICE_IMPACT_LD,'-') SERVICE_IMPACT_LD,
       nvl(Q5.SRV_IND_DCSD_FLG,'-') SRV_IND_DCSD_FLG,
       IND.SRVC_IND_ACT_TERM,
       IND.SRVC_IND_ACTIVE_DT, 
       nvl(AT.DESCRSHORT, '-') SRVC_IND_ACT_TERM_SDESC,
       nvl(AT.DESCR, '-') SRVC_IND_ACT_TERM_DESC,
       IND.SRVC_IND_CD,
       nvl(Q5.SRVC_IND_SD,'-') SRVC_IND_SD,
       nvl(Q5.SRVC_IND_LD,'-') SRVC_IND_LD,
       IND.SRVC_IND_REASON,
       nvl(Q6.SRVC_IND_REASON_SD,'-') SRVC_IND_REASON_SD,
       nvl(Q6.SRVC_IND_REASON_LD,'-') SRVC_IND_REASON_LD,
       IND.SRVC_IND_REFRNCE,
       nvl(Q4.SYSTEM_FUNCTION_FLG,'-') SYSTEM_FUNCTION_FLG,
       nvl(Q4.TERM_CATEGORY,'-') TERM_CATEGORY,
       IND.COMM_COMMENTS,
       IND.DATA_ORIGIN   
  from CSSTG_OWNER.PS_SRVC_IND_DATA IND
  left outer join PS_D_INSTITUTION I
    on IND.INSTITUTION = I.INSTITUTION_CD
   and IND.SRC_SYS_ID = I.SRC_SYS_ID  
  left outer join PS_D_PERSON P
    on IND.EMPLID = P.PERSON_ID
   and IND.SRC_SYS_ID = P.SRC_SYS_ID  
  left outer join Q4    
    on IND.INSTITUTION = Q4.INSTITUTION_CD
   and IND.SRVC_IND_CD = Q4.SRVC_IND_CD
   and IND.SRC_SYS_ID = Q4.SRC_SYS_ID
  left outer join Q5    
    on IND.INSTITUTION = Q5.INSTITUTION_CD
   and IND.SRVC_IND_CD = Q5.SRVC_IND_CD
   and IND.SRC_SYS_ID = Q5.SRC_SYS_ID
   and Q5.Q_ORDER = 1
  left outer join Q6    
    on IND.INSTITUTION = Q6.INSTITUTION_CD
   and IND.SRVC_IND_CD = Q6.SRVC_IND_CD
   and IND.SRVC_IND_REASON = Q6.SRVC_IND_REASON
   and IND.SRC_SYS_ID = Q6.SRC_SYS_ID
   and Q6.Q_ORDER = 1
  left outer join Q1 ST 
    on IND.SCC_SI_END_TERM = ST.TERM_CD
   and IND.SRC_SYS_ID = ST.SRC_SYS_ID
  left outer join Q1 AT 
    on IND.SRVC_IND_ACT_TERM = AT.TERM_CD
   and IND.SRC_SYS_ID = AT.SRC_SYS_ID
 where IND.DATA_ORIGIN <> 'D'
)                                        
select /*+ parallel(8) */
       INSTITUTION_CD, PERSON_ID, SRVC_IND_DTTM, SRC_SYS_ID, 
       EFFDT, INSTITUTION_SID, PERSON_SID, AMOUNT, CONTACT, CONTACT_ID, 
       DFLT_SRVC_IND_RSN, OPRID, PLACED_METHOD, PLACED_PERSON, PLACED_PERSON_ID, PLACED_PROCESS, 
       POS_SRVC_IMPACT_FLG, POS_SRVC_IND_FLG, POS_SRVC_INDICATOR, 
       SCC_DFLT_ACTDATE_FLG, SCC_DFLT_ACTTERM_FLG, SCC_HOLD_DISP_FLG, SCC_IMPACT_DATE_FLG, SCC_IMPACT_TERM_FLG, 
       SCC_SI_END_TERM, SCC_SI_END_DT, SCC_SI_END_TERM_SDESC, SCC_SI_END_TERM_DESC, SCC_SI_ORG_FLG, SCC_SI_PERS_FLG, 
       SERVICE_IMPACT, SERVICE_IMPACT_SD, SERVICE_IMPACT_LD, 
       SRV_IND_DCSD_FLG, SRVC_IND_ACT_TERM, SRVC_IND_ACTIVE_DT, SRVC_IND_ACT_TERM_SDESC, SRVC_IND_ACT_TERM_DESC, 
       SRVC_IND_CD, SRVC_IND_SD, SRVC_IND_LD, SRVC_IND_REASON, SRVC_IND_REASON_SD, SRVC_IND_REASON_LD, 
       SRVC_IND_REFRNCE, SYSTEM_FUNCTION_FLG, TERM_CATEGORY, COMM_COMMENTS, 
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from S
 union all
 select '-', '-', TO_TIMESTAMP('01/01/1900','fmMMfm/fmDDfm/YYYY'), 'CS90', 
        NULL, 2147483646, 2147483646, 0, '-', '-', 
        '-', '-', '-', '-', '-', 
        '-', '-', '-', '-', '-',
        '-', '-', '-', '-', '-', 
        NULL, '-', '-', '-', '-',  
        '-', '-', '-', '-', '-', NULL, 
        '-', '-', '-', '-', '-', 
        '-', '-', '-', '-', '-', 
        '-', '', 'S', SYSDATE, SYSDATE
   from DUAL;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;
 
strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_SRVC_IND rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_SRVC_IND',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_PERSON_SRVC_IND';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_SRVC_IND enable constraint PK_UM_D_PERSON_SRVC_IND';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_PERSON_SRVC_IND');

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

END UM_D_PERSON_SRVC_IND_P;
/
