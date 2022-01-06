CREATE OR REPLACE PROCEDURE             "UM_D_CRSE_ATTR_VAL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
--George Adams
--OLD tables               -- UM_D_CRSE_ATTR_VAL/UM_D_CRSE_ATTR_VAL_VW
--Dependent on             -- UM_D_CRSE (RUN ORDER 200)
--Loads target table       -- UM_D_CRSE_ATTR_VAL
-- V01 4/17/2018           -- srikanth ,pabbu converted to proc from sql
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_CRSE_ATTR_VAL';
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

strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_CRSE_ATTR_VAL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_CRSE_ATTR_VAL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_CRSE_ATTR_VAL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_CRSE_ATTR_VAL');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_CRSE_ATTR_VAL disable constraint PK_UM_D_CRSE_ATTR_VAL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_CRSE_ATTR_VAL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_CRSE_ATTR_VAL';				
insert /*+ append parallel(8) enable_parallel_dml */ into CSMRT_OWNER.UM_D_CRSE_ATTR_VAL
with Q1 as (  
select /*+ parallel(8) inline */ 
       CRSE_ATTR, SRC_SYS_ID, EFFDT, EFF_STATUS, 
       DESCRSHORT CRSE_ATTR_SD, DESCR CRSE_ATTR_LD,  
       DATA_ORIGIN,
       row_number() over (partition by CRSE_ATTR, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_CRSE_ATTR_TBL
 where DATA_ORIGIN <> 'D'), 
       Q2 as (  
select /*+ parallel(8) inline */ 
       CRSE_ATTR, CRSE_ATTR_VALUE, SRC_SYS_ID, EFFDT, 
       DESCR CRSE_ATTR_VALUE_LD, DESCRFORMAL CRSE_ATTR_VALUE_FD, 
       CATALOG_PRINT CATALOG_PRINT_FLG, SCHEDULE_PRINT SCHEDULE_PRINT_FLG, 
       DATA_ORIGIN,
       row_number() over (partition by CRSE_ATTR, CRSE_ATTR_VALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_CRSE_ATTR_VALUE
 where DATA_ORIGIN <> 'D'), 
       Q3 as (  
select /*+ parallel(8) inline */ 
       CRSE_ID CRSE_CD, CRSE_ATTR, CRSE_ATTR_VALUE, SRC_SYS_ID, EFFDT, 
       DATA_ORIGIN,
       row_number() over (partition by CRSE_ID, CRSE_ATTR, CRSE_ATTR_VALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_CRSE_ATTRIBUTES
 where DATA_ORIGIN <> 'D'), 
       S as (
select /*+ parallel(8) inline */ C.CRSE_CD, C.CRSE_OFFER_NUM, nvl(Q3.CRSE_ATTR,'-') CRSE_ATTR, nvl(Q2.CRSE_ATTR_VALUE,'-') CRSE_ATTR_VALUE, C.SRC_SYS_ID, 
       nvl(Q3.EFFDT,to_date('01-JAN-1900')) EFFDT, C.CRSE_SID,
       nvl(Q1.CRSE_ATTR_SD,'-') CRSE_ATTR_SD, nvl(Q1.CRSE_ATTR_LD,'-') CRSE_ATTR_LD, 
       nvl(Q2.CRSE_ATTR_VALUE_LD,'-') CRSE_ATTR_VALUE_LD, nvl(Q2.CRSE_ATTR_VALUE_FD,'-') CRSE_ATTR_VALUE_FD, 
       nvl(Q2.CATALOG_PRINT_FLG,'-') CATALOG_PRINT_FLG, nvl(Q2.SCHEDULE_PRINT_FLG,'-') SCHEDULE_PRINT_FLG, 
       least(C.DATA_ORIGIN,nvl(Q3.DATA_ORIGIN,'Z')) DATA_ORIGIN  
  from CSMRT_OWNER.UM_D_CRSE C
  left outer join Q3
    on C.CRSE_CD = Q3.CRSE_CD
   and C.EFFDT = Q3.EFFDT       -- Oct 2019 
   and C.SRC_SYS_ID = Q3.SRC_SYS_ID
   and Q3.Q_ORDER = 1
  left outer join Q1 
    on Q3.CRSE_ATTR = Q1.CRSE_ATTR
   and Q3.SRC_SYS_ID = Q1.SRC_SYS_ID
   and Q1.Q_ORDER = 1
  left outer join Q2  
    on Q3.CRSE_ATTR = Q2.CRSE_ATTR
   and Q3.CRSE_ATTR_VALUE = Q2.CRSE_ATTR_VALUE
   and Q3.SRC_SYS_ID = Q2.SRC_SYS_ID
   and Q2.Q_ORDER = 1
 where C.DATA_ORIGIN <> 'D' 
    )                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
select /*+ parallel(8) inline */ ROWNUM CRSE_ATTR_VAL_SID, CRSE_CD, CRSE_OFFER_NUM, CRSE_ATTR, CRSE_ATTR_VALUE, SRC_SYS_ID, 
       EFFDT, CRSE_SID, CRSE_ATTR_SD, CRSE_ATTR_LD, CRSE_ATTR_VALUE_LD, CRSE_ATTR_VALUE_FD, 
       CATALOG_PRINT_FLG, SCHEDULE_PRINT_FLG, 
       'S' DATA_ORIGIN, SYSDATE CREATED_EW_DTTM, SYSDATE LASTUPD_EW_DTTM
  from S
-- where not (CRSE_ATTR <> '-' and CRSE_ATTR_VALUE = '-')       -- Oct 2019 
;                    

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_CRSE_ATTR_VAL rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_CRSE_ATTR_VAL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_CRSE_ATTR_VAL enable constraint PK_UM_D_CRSE_ATTR_VAL';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_CRSE_ATTR_VAL');

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

END UM_D_CRSE_ATTR_VAL_P;
/
