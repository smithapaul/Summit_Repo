DROP PROCEDURE CSMRT_OWNER.PS_R_ACAD_PLAN_OWNER_P
/

--
-- PS_R_ACAD_PLAN_OWNER_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_R_ACAD_PLAN_OWNER_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_R_ACAD_PLAN_OWNER from PeopleSoft table PS_ACAD_PLAN_OWNER.
--
 --V01  SMT-xxxx 06/04/2018,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_R_ACAD_PLAN_OWNER';
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

strMessage01    := 'Truncating table CSMRT_OWNER.PS_R_ACAD_PLAN_OWNER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlDynamic   := 'truncate table CSMRT_OWNER.PS_R_ACAD_PLAN_OWNER';
strSqlCommand   := 'SMT_UTILITY.EXECUTE_IMMEDIATE: ' || strSqlDynamic;
COMMON_OWNER.SMT_UTILITY.EXECUTE_IMMEDIATE
                (
                i_SqlStatement          => strSqlDynamic,
                i_MaxTries              => 10,
                i_WaitSeconds           => 10,
                o_Tries                 => intTries
                );

strMessage01    := 'Inserting data into CSMRT_OWNER.PS_R_ACAD_PLAN_OWNER';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'insert into CSMRT_OWNER.PS_R_ACAD_PLAN_OWNER';
insert into CSMRT_OWNER.PS_R_ACAD_PLAN_OWNER 
  with Q0 as (
select distinct 
       INSTITUTION, ACAD_PLAN, EFFDT, SRC_SYS_ID
  from CSSTG_OWNER.PS_ACAD_PLAN_OWNER
 where DATA_ORIGIN <> 'D'),
       Q1 as (
select distinct 
       INSTITUTION, ACAD_PLAN, EFFDT, SRC_SYS_ID,
       decode(max(EFFDT) over (partition by INSTITUTION, ACAD_PLAN, SRC_SYS_ID
                                   order by EFFDT 
                                   rows between unbounded preceding and 1 preceding),NULL,to_date('01-JAN-1800'),EFFDT) EFFDT_START,   
       nvl(min(EFFDT-1) over (partition by INSTITUTION, ACAD_PLAN, SRC_SYS_ID
                                  order by EFFDT
                                  rows between 1 following and unbounded following),to_date('31-DEC-9999')) EFFDT_END,   
       row_number() over (partition by INSTITUTION, ACAD_PLAN, SRC_SYS_ID 
                              order by EFFDT desc) EFFDT_ORDER 
  from Q0),
       Q2 as (
select INSTITUTION, ACAD_PLAN, EFFDT, ACAD_ORG, SRC_SYS_ID,
       PERCENT_OWNED
  from CSSTG_OWNER.PS_ACAD_PLAN_OWNER
 where DATA_ORIGIN <> 'D')
select Q2.INSTITUTION, Q2.ACAD_PLAN, Q2.EFFDT, Q2.ACAD_ORG, Q2.SRC_SYS_ID, 
       Q1.EFFDT_START, Q1.EFFDT_END, Q1.EFFDT_ORDER,  
       nvl(I.INSTITUTION_SID, 2147483646) INSTITUTION_SID, 
       nvl(P.ACAD_PLAN_SID, 2147483646) ACAD_PLAN_SID, 
       nvl(O.ACAD_ORG_SID, 2147483646) ACAD_ORG_SID, 
       Q2.PERCENT_OWNED,
       'S' DATA_ORIGIN,
       SYSDATE CREATED_EW_DTTM,
       SYSDATE LASTUPD_EW_DTTM
  from Q2
  join Q1
    on Q2.INSTITUTION = Q1.INSTITUTION 
   and Q2.ACAD_PLAN = Q1.ACAD_PLAN 
   and Q2.EFFDT = Q1.EFFDT 
   and Q2.SRC_SYS_ID = Q1.SRC_SYS_ID
  left outer join CSMRT_OWNER.PS_D_INSTITUTION I 
    on Q2.INSTITUTION = I.INSTITUTION_CD
   and Q2.SRC_SYS_ID = I.SRC_SYS_ID
   and I.DATA_ORIGIN <> 'D'
  left outer join CSMRT_OWNER.UM_D_ACAD_PLAN P   
    on Q2.INSTITUTION = P.INSTITUTION_CD
   and Q2.ACAD_PLAN = P.ACAD_PLAN_CD
   and Q2.SRC_SYS_ID = P.SRC_SYS_ID
   and P.DATA_ORIGIN <> 'D'
   and P.EFFDT_ORDER = 1
  left outer join PS_D_ACAD_ORG O 
    on Q2.INSTITUTION = O.INSTITUTION_CD
   and Q2.ACAD_ORG = O.ACAD_ORG_CD
   and Q2.SRC_SYS_ID = O.SRC_SYS_ID
   and O.DATA_ORIGIN <> 'D'
   and O.EFFDT_ORDER = 1
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_R_ACAD_PLAN_OWNER rows inserted: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_R_ACAD_PLAN_OWNER',
                i_Action            => 'INSERT',
                i_RowCount          => intRowCount
        );

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_R_ACAD_PLAN_OWNER',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

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

END PS_R_ACAD_PLAN_OWNER_P;
/
