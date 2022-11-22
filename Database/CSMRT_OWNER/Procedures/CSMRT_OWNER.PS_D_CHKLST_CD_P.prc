DROP PROCEDURE CSMRT_OWNER.PS_D_CHKLST_CD_P
/

--
-- PS_D_CHKLST_CD_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_CHKLST_CD_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_CHKLST_CD from PeopleSoft table PS_D_CHKLST_CD.
--
 --V01  SMT-xxxx 10/30/2017,    James Doucette
--                              Converted from DataStage
--V02 2/11/2021              -- Srikanth,Pabbu made changes to CHKLIST_CD_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_CHKLST_CD';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_CHKLST_CD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_CHKLST_CD';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_CHKLST_CD T                                                                                                                                                                                                
using (                                                                                                                                                                                                                                                         
  with Q1 as (  
select INSTITUTION INSTITUTION_CD, CHECKLIST_CD, SRC_SYS_ID, EFFDT, EFF_STATUS EFF_STAT_CD, 
       DESCRSHORT CHKLIST_CD_SD, DESCR CHKLIST_CD_LD, 
       ADMIN_FUNCTION, COMM_KEY, DEFAULT_DUE_DT, DUE_DAYS, SCC_CHECKLIST_TYPE, SCC_TODO_SS_DISP, TRACKING_GROUP, 
       DATA_ORIGIN,  
       row_number() over (partition by INSTITUTION, CHECKLIST_CD, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q1_ORDER
  from CSSTG_OWNER.PS_CS_CHKLST_TBL),
       Q2 as (
select INSTITUTION INSTITUTION_CD, TRACKING_GROUP, SRC_SYS_ID, 
       DESCRSHORT TRACKING_GROUP_SD, DESCR TRACKING_GROUP_LD,  
       row_number() over (partition by INSTITUTION, TRACKING_GROUP, SRC_SYS_ID 
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q2_ORDER 
  from CSSTG_OWNER.PS_TRACK_GRP_TBL
 where DATA_ORIGIN <> 'D'),
       S as ( 
select Q1.INSTITUTION_CD, Q1.CHECKLIST_CD, Q1.SRC_SYS_ID, Q1.EFFDT, Q1.EFF_STAT_CD, 
       Q1.CHKLIST_CD_SD, Q1.CHKLIST_CD_LD, Q1.ADMIN_FUNCTION, Q1.COMM_KEY, Q1.DEFAULT_DUE_DT, Q1.DUE_DAYS, Q1.SCC_CHECKLIST_TYPE, Q1.SCC_TODO_SS_DISP, 
       Q1.TRACKING_GROUP, nvl(Q2.TRACKING_GROUP_SD,'-') TRACKING_GROUP_SD, nvl(Q2.TRACKING_GROUP_LD,'-') TRACKING_GROUP_LD, 
       Q1.DATA_ORIGIN
  from Q1
  left outer join Q2
    on Q1.INSTITUTION_CD = Q2.INSTITUTION_CD
   and Q1.TRACKING_GROUP = Q2.TRACKING_GROUP
   and Q1.SRC_SYS_ID = Q2.SRC_SYS_ID
   and Q2.Q2_ORDER = 1 
 where Q1.Q1_ORDER = 1)                                                                                                                                                                                              
select nvl(D.CHKLIST_CD_SID, --max(D.CHKLIST_CD_SID) over (partition by 1) + This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/11/2021
 (select nvl(max(CHKLIST_CD_SID),0) from CSMRT_OWNER.PS_D_CHKLST_CD where CHKLIST_CD_SID <> 2147483646) +                                                                                                                                                                                      
       row_number() over (partition by 1 order by D.CHKLIST_CD_SID nulls first)) CHKLIST_CD_SID,                                                                                                                                                                
       nvl(D.CHECKLIST_CD, S.CHECKLIST_CD) CHECKLIST_CD,                                                                                                                                                                                                        
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD,                                                                                                                                                                                                  
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,                                                                                                                                                                                                        
       decode(D.EFF_STAT_CD, S.EFF_STAT_CD, D.EFF_STAT_CD, S.EFF_STAT_CD) EFF_STAT_CD,                                                                                                                                                                          
       decode(D.CHKLIST_CD_SD, S.CHKLIST_CD_SD, D.CHKLIST_CD_SD, S.CHKLIST_CD_SD) CHKLIST_CD_SD,                                                                                                                                                                
       decode(D.CHKLIST_CD_LD, S.CHKLIST_CD_LD, D.CHKLIST_CD_LD, S.CHKLIST_CD_LD) CHKLIST_CD_LD,                                                                                                                                                                
       decode(D.ADMIN_FUNCTION, S.ADMIN_FUNCTION, D.ADMIN_FUNCTION, S.ADMIN_FUNCTION) ADMIN_FUNCTION,                                                                                                                                                           
       decode(D.COMM_KEY, S.COMM_KEY, D.COMM_KEY, S.COMM_KEY) COMM_KEY,                                                                                                                                                                                         
       decode(D.DEFAULT_DUE_DT, S.DEFAULT_DUE_DT, D.DEFAULT_DUE_DT, S.DEFAULT_DUE_DT) DEFAULT_DUE_DT,                                                                                                                                                           
       decode(D.DUE_DAYS, S.DUE_DAYS, D.DUE_DAYS, S.DUE_DAYS) DUE_DAYS,                                                                                                                                                                                         
       decode(D.SCC_CHECKLIST_TYPE, S.SCC_CHECKLIST_TYPE, D.SCC_CHECKLIST_TYPE, S.SCC_CHECKLIST_TYPE) SCC_CHECKLIST_TYPE,                                                                                                                                       
       decode(D.SCC_TODO_SS_DISP, S.SCC_TODO_SS_DISP, D.SCC_TODO_SS_DISP, S.SCC_TODO_SS_DISP) SCC_TODO_SS_DISP,                                                                                                                                                 
       decode(D.TRACKING_GROUP, S.TRACKING_GROUP, D.TRACKING_GROUP, S.TRACKING_GROUP) TRACKING_GROUP,                                                                                                                                                           
       decode(D.TRACKING_GROUP_SD, S.TRACKING_GROUP_SD, D.TRACKING_GROUP_SD, S.TRACKING_GROUP_SD) TRACKING_GROUP_SD,                                                                                                                                            
       decode(D.TRACKING_GROUP_LD, S.TRACKING_GROUP_LD, D.TRACKING_GROUP_LD, S.TRACKING_GROUP_LD) TRACKING_GROUP_LD,                                                                                                                                            
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_CHKLST_CD D                                                                                                                                                                                                              
    on D.CHKLIST_CD_SID <> 2147483646                                                                                                                                                                                                                           
   and D.CHECKLIST_CD = S.CHECKLIST_CD                                                                                                                                                                                                                          
   and D.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                      
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                    
   and  T.CHECKLIST_CD = S.CHECKLIST_CD                                                                                                                                                                                                                         
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                       
       T.EFF_STAT_CD = S.EFF_STAT_CD,                                                                                                                                                                                                                           
       T.CHKLIST_CD_SD = S.CHKLIST_CD_SD,                                                                                                                                                                                                                       
       T.CHKLIST_CD_LD = S.CHKLIST_CD_LD,                                                                                                                                                                                                                       
       T.ADMIN_FUNCTION = S.ADMIN_FUNCTION,                                                                                                                                                                                                                     
       T.COMM_KEY = S.COMM_KEY,                                                                                                                                                                                                                                 
       T.DEFAULT_DUE_DT = S.DEFAULT_DUE_DT,                                                                                                                                                                                                                     
       T.DUE_DAYS = S.DUE_DAYS,                                                                                                                                                                                                                                 
       T.SCC_CHECKLIST_TYPE = S.SCC_CHECKLIST_TYPE,                                                                                                                                                                                                             
       T.SCC_TODO_SS_DISP = S.SCC_TODO_SS_DISP,                                                                                                                                                                                                                 
       T.TRACKING_GROUP = S.TRACKING_GROUP,                                                                                                                                                                                                                     
       T.TRACKING_GROUP_SD = S.TRACKING_GROUP_SD,                                                                                                                                                                                                               
       T.TRACKING_GROUP_LD = S.TRACKING_GROUP_LD,                                                                                                                                                                                                               
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                       
       decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or                                                                                                                                                                                                           
       decode(T.CHKLIST_CD_SD,S.CHKLIST_CD_SD,0,1) = 1 or                                                                                                                                                                                                       
       decode(T.CHKLIST_CD_LD,S.CHKLIST_CD_LD,0,1) = 1 or                                                                                                                                                                                                       
       decode(T.ADMIN_FUNCTION,S.ADMIN_FUNCTION,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.COMM_KEY,S.COMM_KEY,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.DEFAULT_DUE_DT,S.DEFAULT_DUE_DT,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.DUE_DAYS,S.DUE_DAYS,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.SCC_CHECKLIST_TYPE,S.SCC_CHECKLIST_TYPE,0,1) = 1 or                                                                                                                                                                                             
       decode(T.SCC_TODO_SS_DISP,S.SCC_TODO_SS_DISP,0,1) = 1 or                                                                                                                                                                                                 
       decode(T.TRACKING_GROUP,S.TRACKING_GROUP,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.TRACKING_GROUP_SD,S.TRACKING_GROUP_SD,0,1) = 1 or                                                                                                                                                                                               
       decode(T.TRACKING_GROUP_LD,S.TRACKING_GROUP_LD,0,1) = 1 or                                                                                                                                                                                               
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.CHKLIST_CD_SID,                                                                                                                                                                                                                                        
       T.CHECKLIST_CD,                                                                                                                                                                                                                                          
       T.INSTITUTION_CD,                                                                                                                                                                                                                                        
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.EFFDT,                                                                                                                                                                                                                                                 
       T.EFF_STAT_CD,                                                                                                                                                                                                                                           
       T.CHKLIST_CD_SD,                                                                                                                                                                                                                                         
       T.CHKLIST_CD_LD,                                                                                                                                                                                                                                         
       T.ADMIN_FUNCTION,                                                                                                                                                                                                                                        
       T.COMM_KEY,                                                                                                                                                                                                                                              
       T.DEFAULT_DUE_DT,                                                                                                                                                                                                                                        
       T.DUE_DAYS,                                                                                                                                                                                                                                              
       T.SCC_CHECKLIST_TYPE,                                                                                                                                                                                                                                    
       T.SCC_TODO_SS_DISP,                                                                                                                                                                                                                                      
       T.TRACKING_GROUP,                                                                                                                                                                                                                                        
       T.TRACKING_GROUP_SD,                                                                                                                                                                                                                                     
       T.TRACKING_GROUP_LD,                                                                                                                                                                                                                                     
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.CHKLIST_CD_SID,                                                                                                                                                                                                                                        
       S.CHECKLIST_CD,                                                                                                                                                                                                                                          
       S.INSTITUTION_CD,                                                                                                                                                                                                                                        
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.EFFDT,                                                                                                                                                                                                                                                 
       S.EFF_STAT_CD,                                                                                                                                                                                                                                           
       S.CHKLIST_CD_SD,                                                                                                                                                                                                                                         
       S.CHKLIST_CD_LD,                                                                                                                                                                                                                                         
       S.ADMIN_FUNCTION,                                                                                                                                                                                                                                        
       S.COMM_KEY,                                                                                                                                                                                                                                              
       S.DEFAULT_DUE_DT,                                                                                                                                                                                                                                        
       S.DUE_DAYS,                                                                                                                                                                                                                                              
       S.SCC_CHECKLIST_TYPE,                                                                                                                                                                                                                                    
       S.SCC_TODO_SS_DISP,                                                                                                                                                                                                                                      
       S.TRACKING_GROUP,                                                                                                                                                                                                                                        
       S.TRACKING_GROUP_SD,                                                                                                                                                                                                                                     
       S.TRACKING_GROUP_LD,                                                                                                                                                                                                                                     
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;                                                                                                                                                                                                                                             

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_CHKLST_CD rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_CHKLST_CD',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_CHKLST_CD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_CHKLST_CD';
update CSMRT_OWNER.PS_D_CHKLST_CD T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.CHKLIST_CD_SID < 2147483646
   and not exists (select 1 
                     from CSSTG_OWNER.PS_CS_CHKLST_TBL S
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.CHECKLIST_CD = S.CHECKLIST_CD
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_CHKLST_CD rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_CHKLST_CD',
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

END PS_D_CHKLST_CD_P;
/
