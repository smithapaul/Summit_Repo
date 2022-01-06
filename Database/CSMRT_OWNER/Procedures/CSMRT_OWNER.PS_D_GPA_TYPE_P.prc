CREATE OR REPLACE PROCEDURE             "PS_D_GPA_TYPE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_GPA_TYPE from PeopleSoft table PS_D_GPA_TYPE.
--
--V01  SMT-xxxx 11/08/2017,     James Doucette
--                              Converted from DataStage
--V01.a   SMT-xxxx 03/15/2018,  James Doucette
--                              Updated INSTITUTION field to INSTITUTION_CD.
--V02 2/12/2021              -- Srikanth,Pabbu made changes to GPA_TYPE_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_GPA_TYPE';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_GPA_TYPE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_GPA_TYPE';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_GPA_TYPE T                                                                                                                                                                                                 
using (                                                                                                                                                                                                                                                         
  with Q1 as (  
select INSTITUTION INSTITUTION_CD, GPA_TYPE GPA_TYPE_ID, SRC_SYS_ID, EFFDT, EFF_STATUS EFF_STAT_CD, 
       DESCRSHORT GPA_TYPE_SD, DESCR GPA_TYPE_LD,  
       DATA_ORIGIN,  
       row_number() over (partition by INSTITUTION, GPA_TYPE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_GPA_TYPE_TBL),
       S as (
select INSTITUTION_CD, GPA_TYPE_ID, SRC_SYS_ID, EFFDT, EFF_STAT_CD, 
       GPA_TYPE_SD, GPA_TYPE_LD,  
       DATA_ORIGIN  
  from Q1
 where Q1.Q_ORDER = 1)                                                                                                                                                                                              
select nvl(D.GPA_TYPE_SID, --max(D.GPA_TYPE_SID) over (partition by 1) + This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021
 (select nvl(max(GPA_TYPE_SID),0) from CSMRT_OWNER.PS_D_GPA_TYPE where GPA_TYPE_SID <> 2147483646) +                                                                                                                                                                                        
       row_number() over (partition by 1 order by D.GPA_TYPE_SID nulls first)) GPA_TYPE_SID,                                                                                                                                                                    
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD,                                                                                                                                                                                                  
       nvl(D.GPA_TYPE_ID, S.GPA_TYPE_ID) GPA_TYPE_ID,                                                                                                                                                                                                           
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,                                                                                                                                                                                                        
       decode(D.EFF_STAT_CD, S.EFF_STAT_CD, D.EFF_STAT_CD, S.EFF_STAT_CD) EFF_STAT_CD,                                                                                                                                                                          
       decode(D.GPA_TYPE_SD, S.GPA_TYPE_SD, D.GPA_TYPE_SD, S.GPA_TYPE_SD) GPA_TYPE_SD,                                                                                                                                                                          
       decode(D.GPA_TYPE_LD, S.GPA_TYPE_LD, D.GPA_TYPE_LD, S.GPA_TYPE_LD) GPA_TYPE_LD,                                                                                                                                                                          
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_GPA_TYPE D                                                                                                                                                                                                               
    on D.GPA_TYPE_SID <> 2147483646                                                                                                                                                                                                                             
   and D.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                      
   and D.GPA_TYPE_ID = S.GPA_TYPE_ID                                                                                                                                                                                                                            
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                    
   and  T.GPA_TYPE_ID = S.GPA_TYPE_ID                                                                                                                                                                                                                           
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                       
       T.EFF_STAT_CD = S.EFF_STAT_CD,                                                                                                                                                                                                                           
       T.GPA_TYPE_SD = S.GPA_TYPE_SD,                                                                                                                                                                                                                           
       T.GPA_TYPE_LD = S.GPA_TYPE_LD,                                                                                                                                                                                                                           
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                       
       decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or                                                                                                                                                                                                           
       decode(T.GPA_TYPE_SD,S.GPA_TYPE_SD,0,1) = 1 or                                                                                                                                                                                                           
       decode(T.GPA_TYPE_LD,S.GPA_TYPE_LD,0,1) = 1 or                                                                                                                                                                                                           
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.GPA_TYPE_SID,                                                                                                                                                                                                                                          
       T.INSTITUTION_CD,                                                                                                                                                                                                                                        
       T.GPA_TYPE_ID,                                                                                                                                                                                                                                           
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.EFFDT,                                                                                                                                                                                                                                                 
       T.EFF_STAT_CD,                                                                                                                                                                                                                                           
       T.GPA_TYPE_SD,                                                                                                                                                                                                                                           
       T.GPA_TYPE_LD,                                                                                                                                                                                                                                           
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.GPA_TYPE_SID,                                                                                                                                                                                                                                          
       S.INSTITUTION_CD,                                                                                                                                                                                                                                        
       S.GPA_TYPE_ID,                                                                                                                                                                                                                                           
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.EFFDT,                                                                                                                                                                                                                                                 
       S.EFF_STAT_CD,                                                                                                                                                                                                                                           
       S.GPA_TYPE_SD,                                                                                                                                                                                                                                           
       S.GPA_TYPE_LD,                                                                                                                                                                                                                                           
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;                                                                                                                                                                                                                                                         

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_GPA_TYPE rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_GPA_TYPE',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_GPA_TYPE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_GPA_TYPE';
update CSMRT_OWNER.PS_D_GPA_TYPE T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.GPA_TYPE_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_GPA_TYPE_TBL S
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.GPA_TYPE_ID = S.GPA_TYPE
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_GPA_TYPE rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_GPA_TYPE',
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

END PS_D_GPA_TYPE_P;
/
