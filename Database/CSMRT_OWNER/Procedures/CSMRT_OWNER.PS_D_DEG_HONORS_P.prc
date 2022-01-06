CREATE OR REPLACE PROCEDURE             "PS_D_DEG_HONORS_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_DEG_HONORS from PeopleSoft table PS_D_DEG_HONORS.
--
 --V01  SMT-xxxx 11/01/2017,    James Doucette
--                              Converted from DataStage
--V02 2/12/2021            -- Srikanth,Pabbu made changes to DEG_HONORS_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_DEG_HONORS';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_DEG_HONORS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_DEG_HONORS';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_DEG_HONORS T                                                                                                                                                                                               
using (                                                                                                                                                                                                                                                         
  with X1 as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as (  
select INSTITUTION INSTITUTION_CD, HONORS_TYPE HONORS_TYPE_CD, HONORS_CODE HONORS_CD, SRC_SYS_ID, EFFDT, EFF_STATUS EFF_STAT_CD, 
       DESCRSHORT HONORS_SD, DESCR HONORS_LD, DESCRFORMAL HONORS_FD,
       DATA_ORIGIN,  
       row_number() over (partition by INSTITUTION, HONORS_TYPE, HONORS_CODE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q1_ORDER
  from CSSTG_OWNER.PS_DEGR_HONORS_TBL),
       S as (
select Q1.INSTITUTION_CD, Q1.HONORS_TYPE_CD, Q1.HONORS_CD, Q1.SRC_SYS_ID, Q1.EFFDT, Q1.EFF_STAT_CD, 
       nvl(X1.XLATSHORTNAME,'-') HONORS_TYPE_SD, nvl(X1.XLATLONGNAME,'-') HONORS_TYPE_LD, 
       Q1.HONORS_SD, Q1.HONORS_LD, Q1.HONORS_FD,  
       Q1.DATA_ORIGIN  
  from Q1
  left outer join X1
    on Q1.HONORS_TYPE_CD = X1.FIELDVALUE
   and Q1.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'HONORS_TYPE' 
   and X1.X_ORDER = 1  
 where Q1_ORDER = 1)                                                                                                                                                                                              
select nvl(D.DEG_HONORS_SID, --max(D.DEG_HONORS_SID) over (partition by 1) +   This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021
 (select nvl(max(DEG_HONORS_SID),0) from CSMRT_OWNER.PS_D_DEG_HONORS where DEG_HONORS_SID <> 2147483646) +                                                                                                                                                                                    
       row_number() over (partition by 1 order by D.DEG_HONORS_SID nulls first)) DEG_HONORS_SID,                                                                                                                                                                
       nvl(D.HONORS_CD, S.HONORS_CD) HONORS_CD,                                                                                                                                                                                                                 
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD,                                                                                                                                                                                                  
       nvl(D.HONORS_TYPE_CD, S.HONORS_TYPE_CD) HONORS_TYPE_CD,                                                                                                                                                                                                  
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,                                                                                                                                                                                                        
       decode(D.EFF_STAT_CD, S.EFF_STAT_CD, D.EFF_STAT_CD, S.EFF_STAT_CD) EFF_STAT_CD,                                                                                                                                                                          
       decode(D.HONORS_TYPE_SD, S.HONORS_TYPE_SD, D.HONORS_TYPE_SD, S.HONORS_TYPE_SD) HONORS_TYPE_SD,                                                                                                                                                           
       decode(D.HONORS_TYPE_LD, S.HONORS_TYPE_LD, D.HONORS_TYPE_LD, S.HONORS_TYPE_LD) HONORS_TYPE_LD,                                                                                                                                                           
       decode(D.HONORS_SD, S.HONORS_SD, D.HONORS_SD, S.HONORS_SD) HONORS_SD,                                                                                                                                                                                    
       decode(D.HONORS_LD, S.HONORS_LD, D.HONORS_LD, S.HONORS_LD) HONORS_LD,                                                                                                                                                                                    
       decode(D.HONORS_FD, S.HONORS_FD, D.HONORS_FD, S.HONORS_FD) HONORS_FD,                                                                                                                                                                                    
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_DEG_HONORS D                                                                                                                                                                                                             
    on D.DEG_HONORS_SID <> 2147483646                                                                                                                                                                                                                           
   and D.HONORS_CD = S.HONORS_CD                                                                                                                                                                                                                                
   and D.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                      
   and D.HONORS_TYPE_CD = S.HONORS_TYPE_CD                                                                                                                                                                                                                      
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                    
   and  T.HONORS_CD = S.HONORS_CD                                                                                                                                                                                                                               
   and  T.HONORS_TYPE_CD = S.HONORS_TYPE_CD                                                                                                                                                                                                                     
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                       
       T.EFF_STAT_CD = S.EFF_STAT_CD,                                                                                                                                                                                                                           
       T.HONORS_TYPE_SD = S.HONORS_TYPE_SD,                                                                                                                                                                                                                     
       T.HONORS_TYPE_LD = S.HONORS_TYPE_LD,                                                                                                                                                                                                                     
       T.HONORS_SD = S.HONORS_SD,                                                                                                                                                                                                                               
       T.HONORS_LD = S.HONORS_LD,                                                                                                                                                                                                                               
       T.HONORS_FD = S.HONORS_FD,                                                                                                                                                                                                                               
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                       
       decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or                                                                                                                                                                                                           
       decode(T.HONORS_TYPE_SD,S.HONORS_TYPE_SD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.HONORS_TYPE_LD,S.HONORS_TYPE_LD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.HONORS_SD,S.HONORS_SD,0,1) = 1 or                                                                                                                                                                                                               
       decode(T.HONORS_LD,S.HONORS_LD,0,1) = 1 or                                                                                                                                                                                                               
       decode(T.HONORS_FD,S.HONORS_FD,0,1) = 1 or                                                                                                                                                                                                               
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.DEG_HONORS_SID,                                                                                                                                                                                                                                        
       T.HONORS_CD,                                                                                                                                                                                                                                             
       T.INSTITUTION_CD,                                                                                                                                                                                                                                        
       T.HONORS_TYPE_CD,                                                                                                                                                                                                                                        
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.EFFDT,                                                                                                                                                                                                                                                 
       T.EFF_STAT_CD,                                                                                                                                                                                                                                           
       T.HONORS_TYPE_SD,                                                                                                                                                                                                                                        
       T.HONORS_TYPE_LD,                                                                                                                                                                                                                                        
       T.HONORS_SD,                                                                                                                                                                                                                                             
       T.HONORS_LD,                                                                                                                                                                                                                                             
       T.HONORS_FD,                                                                                                                                                                                                                                             
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.DEG_HONORS_SID,                                                                                                                                                                                                                                        
       S.HONORS_CD,                                                                                                                                                                                                                                             
       S.INSTITUTION_CD,                                                                                                                                                                                                                                        
       S.HONORS_TYPE_CD,                                                                                                                                                                                                                                        
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.EFFDT,                                                                                                                                                                                                                                                 
       S.EFF_STAT_CD,                                                                                                                                                                                                                                           
       S.HONORS_TYPE_SD,                                                                                                                                                                                                                                        
       S.HONORS_TYPE_LD,                                                                                                                                                                                                                                        
       S.HONORS_SD,                                                                                                                                                                                                                                             
       S.HONORS_LD,                                                                                                                                                                                                                                             
       S.HONORS_FD,                                                                                                                                                                                                                                             
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
; 

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_DEG_HONORS rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_DEG_HONORS',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_DEG_HONORS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_DEG_HONORS';
update CSMRT_OWNER.PS_D_DEG_HONORS T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.DEG_HONORS_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PSXLATITEM S
                    where S.FIELDNAME = 'HONORS_TYPE'
                      and S.FIELDVALUE = T.HONORS_TYPE_CD 
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_DEG_HONORS rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_DEG_HONORS',
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

END PS_D_DEG_HONORS_P;
/
