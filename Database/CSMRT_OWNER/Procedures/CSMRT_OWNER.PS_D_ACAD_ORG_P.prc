CREATE OR REPLACE PROCEDURE             "PS_D_ACAD_ORG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_ACAD_ORG from PeopleSoft table PS_ACAD_ORG_TBL.
--
 --V01  SMT-xxxx 11/22/2017,    James Doucette
--                              Converted from DataStage
-- V02 2/11/2021            -- Srikanth,Pabbu made changes to ACAD_ORG_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_ACAD_ORG';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_ACAD_ORG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_ACAD_ORG';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_ACAD_ORG T                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
using (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
  with D2 as (
select distinct
       ACAD_ORG_SID, ACAD_ORG_CD, SRC_SYS_ID
  from CSMRT_OWNER.PS_D_ACAD_ORG),      -- !!!!!!!!!! 
       N1 as (
select distinct 
       ACAD_ORG ACAD_ORG_CD, SRC_SYS_ID
  from CSSTG_OWNER.PS_ACAD_ORG_TBL
 where DATA_ORIGIN <> 'D'
 minus
select ACAD_ORG_CD, SRC_SYS_ID
  from CSMRT_OWNER.PS_D_ACAD_ORG),      -- !!!!!!!!!!! 
       N2 as (
select max(ACAD_ORG_SID) MAX_SID
  from CSMRT_OWNER.PS_D_ACAD_ORG        -- !!!!!!!!!! 
 where ACAD_ORG_SID <> 2147483646),
       N3 as (
select N1.ACAD_ORG_CD, N1.SRC_SYS_ID, 
       nvl(N2.MAX_SID,0) + row_number() over (partition by 1 order by N1.ACAD_ORG_CD, N1.SRC_SYS_ID nulls first) NEW_SID
  from N1, N2
),
S as (
select STG.EFFDT,
       STG.ACAD_ORG ACAD_ORG_CD,
       STG.SRC_SYS_ID,
       decode(max(STG.EFFDT) over (partition by STG.ACAD_ORG, STG.SRC_SYS_ID
                                       order by STG.EFFDT 
                                   rows between unbounded preceding and 1 preceding),NULL,to_date('01-JAN-1800'),STG.EFFDT) EFFDT_START,   
       nvl(min(STG.EFFDT-1) over (partition by STG.ACAD_ORG, STG.SRC_SYS_ID
                                      order by STG.EFFDT
                                  rows between 1 following and unbounded following),to_date('31-DEC-9999')) EFFDT_END,   
       row_number() over (partition by STG.ACAD_ORG, STG.SRC_SYS_ID 
                              order by STG.EFFDT desc) EFFDT_ORDER, 
       STG.EFF_STATUS EFF_STAT_CD, 
       STG.DESCRSHORT ACAD_ORG_SD, 
       STG.DESCR ACAD_ORG_LD,
       STG.DESCRFORMAL ACAD_ORG_FD,
       STG.ACAD_ORG || ' (' || STG.DESCR || ')' ACAD_ORG_CD_DESC, 
       STG.INSTITUTION INSTITUTION_CD
  from CSSTG_OWNER.PS_ACAD_ORG_TBL STG
 where STG.DATA_ORIGIN <> 'D')
select 
       nvl(nvl(D.ACAD_ORG_SID, D2.ACAD_ORG_SID), N3.NEW_SID) ACAD_ORG_SID, 
       nvl(D.EFFDT, S.EFFDT) EFFDT,                                                
       nvl(D.ACAD_ORG_CD, S.ACAD_ORG_CD) ACAD_ORG_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
       decode(D.EFFDT_START,S.EFFDT_START,D.EFFDT_START,S.EFFDT_START) EFFDT_START,
       decode(D.EFFDT_END,S.EFFDT_END,D.EFFDT_END,S.EFFDT_END) EFFDT_END,
       decode(D.EFFDT_ORDER,S.EFFDT_ORDER,D.EFFDT_ORDER,S.EFFDT_ORDER) EFFDT_ORDER,
       decode(D.EFF_STAT_CD, S.EFF_STAT_CD, D.EFF_STAT_CD, S.EFF_STAT_CD) EFF_STAT_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       decode(D.ACAD_ORG_SD, S.ACAD_ORG_SD, D.ACAD_ORG_SD, S.ACAD_ORG_SD) ACAD_ORG_SD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       decode(D.ACAD_ORG_LD, S.ACAD_ORG_LD, D.ACAD_ORG_LD, S.ACAD_ORG_LD) ACAD_ORG_LD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       decode(D.ACAD_ORG_FD, S.ACAD_ORG_FD, D.ACAD_ORG_FD, S.ACAD_ORG_FD) ACAD_ORG_FD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       decode(D.ACAD_ORG_CD_DESC, S.ACAD_ORG_CD_DESC, D.ACAD_ORG_CD_DESC, S.ACAD_ORG_CD_DESC) ACAD_ORG_CD_DESC,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       decode(D.INSTITUTION_CD, S.INSTITUTION_CD, D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       decode(D.DATA_ORIGIN,'S',D.DATA_ORIGIN,'S') DATA_ORIGIN,
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM, 
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM 
  from S
  left outer join CSMRT_OWNER.PS_D_ACAD_ORG D       -- !!!!!!!!!!! 
    on D.EFFDT = S.EFFDT 
   and D.ACAD_ORG_CD = S.ACAD_ORG_CD
   and D.SRC_SYS_ID = S.SRC_SYS_ID
   and D.ACAD_ORG_SID <> 2147483646 
  left outer join D2
    on S.ACAD_ORG_CD = D2.ACAD_ORG_CD
   and S.SRC_SYS_ID = D2.SRC_SYS_ID 
  left outer join N3
    on S.ACAD_ORG_CD = N3.ACAD_ORG_CD
   and S.SRC_SYS_ID = N3.SRC_SYS_ID) S 
    on (T.EFFDT = S.EFFDT
   and  T.ACAD_ORG_CD = S.ACAD_ORG_CD 
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)
 when matched then update set                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
       T.EFFDT_START = S.EFFDT_START,
       T.EFFDT_END = S.EFFDT_END,
       T.EFFDT_ORDER = S.EFFDT_ORDER,
       T.EFF_STAT_CD = S.EFF_STAT_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.ACAD_ORG_SD = S.ACAD_ORG_SD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.ACAD_ORG_LD = S.ACAD_ORG_LD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.ACAD_ORG_FD = S.ACAD_ORG_FD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.ACAD_ORG_CD_DESC = S.ACAD_ORG_CD_DESC,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.INSTITUTION_CD = S.INSTITUTION_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.LASTUPD_EW_DTTM = S.LASTUPD_EW_DTTM
 where                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       decode(T.EFFDT_START,S.EFFDT_START,0,1) = 1 or 
       decode(T.EFFDT_END,S.EFFDT_END,0,1) = 1 or 
       decode(T.EFFDT_ORDER,S.EFFDT_ORDER,0,1) = 1 or 
       decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       decode(T.ACAD_ORG_SD,S.ACAD_ORG_SD,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       decode(T.ACAD_ORG_LD,S.ACAD_ORG_LD,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       decode(T.ACAD_ORG_FD,S.ACAD_ORG_FD,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       decode(T.ACAD_ORG_CD_DESC,S.ACAD_ORG_CD_DESC,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       decode(T.INSTITUTION_CD,S.INSTITUTION_CD,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  when not matched then                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
insert (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.ACAD_ORG_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       T.EFFDT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.ACAD_ORG_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       T.EFFDT_START,
       T.EFFDT_END,
       T.EFFDT_ORDER,
       T.EFF_STAT_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.ACAD_ORG_SD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.ACAD_ORG_LD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.ACAD_ORG_FD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.ACAD_ORG_CD_DESC,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
       T.INSTITUTION_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
values (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.ACAD_ORG_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       S.EFFDT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       S.ACAD_ORG_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       S.SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       S.EFFDT_START,
       S.EFFDT_END,
       S.EFFDT_ORDER,
       S.EFF_STAT_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       S.ACAD_ORG_SD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       S.ACAD_ORG_LD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       S.ACAD_ORG_FD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       S.ACAD_ORG_CD_DESC,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
       S.INSTITUTION_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       SYSDATE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       SYSDATE)
;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ACAD_ORG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ACAD_ORG',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_ACAD_ORG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_ACAD_ORG';
update CSMRT_OWNER.PS_D_ACAD_ORG T 
   set EFFDT_START = '31-DEC-9999',
       EFFDT_ORDER = 9,
       DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.ACAD_ORG_SID <> 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_ACAD_ORG_TBL S
                    where T.ACAD_ORG_CD = S.ACAD_ORG
                      and T.EFFDT = S.EFFDT
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ACAD_ORG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ACAD_ORG',
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

END PS_D_ACAD_ORG_P;
/
