CREATE OR REPLACE PROCEDURE             "UM_D_PERSON_CITIZEN_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
-- Old Tables              --UM_D_PERSON_CITIZEN / UM_D_PERSON_CS_CITIZEN_USA_VW
-- Loads target table      -- UM_D_PERSON_CITIZEN
-- UM_D_PERSON_CITIZEN -- Dependent on PS_D_PERSON 
-- V01 4/6/2018            -- srikanth ,pabbu converted to proc from sql
-- V02 6/6/2019            -- Doucette, James converted to destructive load from Merge
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_PERSON_CITIZEN';
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

strMessage01    := 'Disabling Indexes for table CSMRT_OWNER.UM_D_PERSON_CITIZEN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);
COMMON_OWNER.SMT_INDEX.ALL_UNUSABLE('CSMRT_OWNER','UM_D_PERSON_CITIZEN');

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_CITIZEN disable constraint PK_UM_D_PERSON_CITIZEN';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
strMessage01    := 'Truncating table CSMRT_OWNER.UM_D_PERSON_CITIZEN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.UM_D_PERSON_CITIZEN';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.UM_D_PERSON_CITIZEN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.UM_D_PERSON_CITIZEN';				
insert /*+ append */ into UM_D_PERSON_CITIZEN
with Q1 as ( 
select /*+ inline */ 
       C.EMPLID PERSON_ID, C.DEPENDENT_ID, C.COUNTRY, C.SRC_SYS_ID, 
       C.CITIZENSHIP_STATUS, nvl(S.DESCRSHORT,'-') CITIZENSHIP_STATUS_SD, nvl(S.DESCR,'-') CITIZENSHIP_STATUS_LD, 
       nvl(T.DESCRSHORT,'-') COUNTRY_SD, nvl(T.DESCR,'-') COUNTRY_LD, nvl(T.COUNTRY_2CHAR,'-') COUNTRY_2CHAR, nvl(T.EU_MEMBER_STATE,'-') EU_MEMBER_STATE, 
       case when C.COUNTRY = 'USA' or C.DATA_ORIGIN <> 'S' 
            then 999
            else row_number() over (partition by C.EMPLID, C.DEPENDENT_ID, C.SRC_SYS_ID
                                        order by C.DATA_ORIGIN desc, decode(C.COUNTRY,'USA',9,0), decode(C.CITIZENSHIP_STATUS,'-','9', C.CITIZENSHIP_STATUS), C.COUNTRY) 
        end NON_ORDER,
       C.DATA_ORIGIN
  from CSSTG_OWNER.PS_CITIZENSHIP C
  left outer join CSSTG_OWNER.PS_CITIZEN_STS_TBL S
    on C.COUNTRY = S.COUNTRY 
   and C.CITIZENSHIP_STATUS = S.CITIZENSHIP_STATUS
   and C.SRC_SYS_ID = S.SRC_SYS_ID
   and S.DATA_ORIGIN <> 'D'
  left outer join CSSTG_OWNER.PS_COUNTRY_TBL T
    on C.COUNTRY = T.COUNTRY 
   and C.SRC_SYS_ID = T.SRC_SYS_ID
   and T.DATA_ORIGIN <> 'D'
 where C.DATA_ORIGIN <> 'D'),
       Q2 as ( 
select /*+ inline */  
       Q1.PERSON_ID, Q1.DEPENDENT_ID, Q1.COUNTRY, Q1.SRC_SYS_ID, 
       Q1.CITIZENSHIP_STATUS, Q1.CITIZENSHIP_STATUS_SD, Q1.CITIZENSHIP_STATUS_LD, 
       Q1.COUNTRY_SD, Q1.COUNTRY_LD, Q1.COUNTRY_2CHAR, Q1.EU_MEMBER_STATE,
       nvl(USA.CITIZENSHIP_STATUS,'-') CITIZENSHIP_STATUS_USA, 
       nvl(USA.CITIZENSHIP_STATUS_SD,'-') CITIZENSHIP_STATUS_SD_USA, 
       nvl(USA.CITIZENSHIP_STATUS_LD,'-') CITIZENSHIP_STATUS_LD_USA, 
       row_number() over (partition by Q1.PERSON_ID, Q1.DEPENDENT_ID, Q1.SRC_SYS_ID
                              order by Q1.DATA_ORIGIN desc, decode(Q1.COUNTRY,'USA',9,0), Q1.NON_ORDER) CIT_ORDER,
       Q1.DATA_ORIGIN
  from Q1
  left outer join Q1 USA 
    on Q1.PERSON_ID = USA.PERSON_ID 
   and Q1.DEPENDENT_ID = USA.DEPENDENT_ID 
   and USA.COUNTRY = 'USA' 
   and Q1.SRC_SYS_ID = USA.SRC_SYS_ID
   and USA.DATA_ORIGIN <> 'D'),
       S as (
select /*+ inline */ 
       P.PERSON_ID, nvl(Q2.DEPENDENT_ID,'-') DEPENDENT_ID, nvl(Q2.COUNTRY,'-') COUNTRY, P.SRC_SYS_ID, 
       P.PERSON_SID,
       nvl(Q2.CITIZENSHIP_STATUS,'-') CITIZENSHIP_STATUS, nvl(Q2.CITIZENSHIP_STATUS_SD,'-') CITIZENSHIP_STATUS_SD, nvl(Q2.CITIZENSHIP_STATUS_LD,'-') CITIZENSHIP_STATUS_LD, 
       nvl(Q2.COUNTRY_SD,'-') COUNTRY_SD, nvl(Q2.COUNTRY_LD,'-') COUNTRY_LD, nvl(Q2.COUNTRY_2CHAR,'-') COUNTRY_2CHAR, nvl(Q2.EU_MEMBER_STATE,'-') EU_MEMBER_STATE, 
       nvl(CITIZENSHIP_STATUS_USA,'-') CITIZENSHIP_STATUS_USA, nvl(CITIZENSHIP_STATUS_SD_USA,'-') CITIZENSHIP_STATUS_SD_USA, nvl(CITIZENSHIP_STATUS_LD_USA,'-') CITIZENSHIP_STATUS_LD_USA,   
       nvl(CIT_ORDER,1) CIT_ORDER,  
       least(P.DATA_ORIGIN,nvl(Q2.DATA_ORIGIN,'Z')) DATA_ORIGIN 
  from PS_D_PERSON P 
  left outer join Q2 
    on P.PERSON_ID = Q2.PERSON_ID
   and P.SRC_SYS_ID = Q2.SRC_SYS_ID) 
select PERSON_ID, 
       DEPENDENT_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
       COUNTRY,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
       PERSON_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       CITIZENSHIP_STATUS,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       CITIZENSHIP_STATUS_SD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       CITIZENSHIP_STATUS_LD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       COUNTRY_SD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       COUNTRY_LD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       COUNTRY_2CHAR,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
       EU_MEMBER_STATE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
       CITIZENSHIP_STATUS_USA,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
       CITIZENSHIP_STATUS_SD_USA,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
       CITIZENSHIP_STATUS_LD_USA,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
       CIT_ORDER,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
       DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       sysdate CREATED_EW_DTTM,
       sysdate LASTUPD_EW_DTTM                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
  from S 
 where DATA_ORIGIN <> 'D'
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_CITIZEN rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_CITIZEN',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Enabling Indexes for table CSMRT_OWNER.UM_D_PERSON_CITIZEN';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'alter table CSMRT_OWNER.UM_D_PERSON_CITIZEN enable constraint PK_UM_D_PERSON_CITIZEN';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );
				
COMMON_OWNER.SMT_INDEX.ALL_REBUILD('CSMRT_OWNER','UM_D_PERSON_CITIZEN');

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

END UM_D_PERSON_CITIZEN_P;
/
