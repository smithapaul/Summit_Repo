DROP PROCEDURE CSMRT_OWNER.PS_D_ADMIN_FUNC_P
/

--
-- PS_D_ADMIN_FUNC_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_ADMIN_FUNC_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_ADMIN_FUNC from PeopleSoft table PS_ADM_FUNCTN_TBL.
--
 --V01  SMT-xxxx 10/12/2017,    James Doucette
--                              Converted from DataStage
--V02 2/11/2021            --   Srikanth,Pabbu made changes to ADMIN_FUNC_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_ADMIN_FUNC';
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


strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_ADMIN_FUNC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_ADMIN_FUNC';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_ADMIN_FUNC T                                                                                                                                                                                               
using (                                                                                                                                                                                                                                                         
  with Q1 as (  
select ADMIN_FUNCTION, SRC_SYS_ID, EFFDT, EFF_STATUS, 
       DESCRSHORT ADMIN_FUNCTION_SD, DESCR ADMIN_FUNCTION_LD, 
       DATA_ORIGIN,  
       row_number() over (partition by ADMIN_FUNCTION, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) TAB_ORDER
  from CSSTG_OWNER.PS_ADM_FUNCTN_TBL),
       S as (
select ADMIN_FUNCTION, SRC_SYS_ID, EFFDT, EFF_STATUS, 
       ADMIN_FUNCTION_SD, ADMIN_FUNCTION_LD, 
       DATA_ORIGIN  
  from Q1
 where TAB_ORDER = 1)                                                                                                                                                                                              
select nvl(D.ADMIN_FUNC_SID, --max(D.ADMIN_FUNC_SID) over (partition by 1) + this code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/11/2021
 (select nvl(max(ADMIN_FUNC_SID),0) from CSMRT_OWNER.PS_D_ADMIN_FUNC where ADMIN_FUNC_SID <> 2147483646) +                                                                                                                                                                                       
       row_number() over (partition by 1 order by D.ADMIN_FUNC_SID nulls first)) ADMIN_FUNC_SID,                                                                                                                                                                
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       nvl(D.ADMIN_FUNCTION, S.ADMIN_FUNCTION) ADMIN_FUNCTION,                                                                                                                                                                                                  
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,                                                                                                                                                                                                        
       decode(D.EFF_STATUS, S.EFF_STATUS, D.EFF_STATUS, S.EFF_STATUS) EFF_STATUS,                                                                                                                                                                               
       decode(D.ADMIN_FUNCTION_SD, S.ADMIN_FUNCTION_SD, D.ADMIN_FUNCTION_SD, S.ADMIN_FUNCTION_SD) ADMIN_FUNCTION_SD,                                                                                                                                            
       decode(D.ADMIN_FUNCTION_LD, S.ADMIN_FUNCTION_LD, D.ADMIN_FUNCTION_LD, S.ADMIN_FUNCTION_LD) ADMIN_FUNCTION_LD,                                                                                                                                            
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_ADMIN_FUNC D                                                                                                                                                                                                             
    on D.ADMIN_FUNC_SID <> 2147483646                                                                                                                                                                                                                           
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
   and D.ADMIN_FUNCTION = S.ADMIN_FUNCTION                                                                                                                                                                                                                      
) S                                                                                                                                                                                                                                                             
    on  (T.ADMIN_FUNCTION = S.ADMIN_FUNCTION                                                                                                                                                                                                                    
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                       
       T.EFF_STATUS = S.EFF_STATUS,                                                                                                                                                                                                                             
       T.ADMIN_FUNCTION_SD = S.ADMIN_FUNCTION_SD,                                                                                                                                                                                                               
       T.ADMIN_FUNCTION_LD = S.ADMIN_FUNCTION_LD,                                                                                                                                                                                                               
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                       
       decode(T.EFF_STATUS,S.EFF_STATUS,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.ADMIN_FUNCTION_SD,S.ADMIN_FUNCTION_SD,0,1) = 1 or                                                                                                                                                                                               
       decode(T.ADMIN_FUNCTION_LD,S.ADMIN_FUNCTION_LD,0,1) = 1 or                                                                                                                                                                                               
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.ADMIN_FUNC_SID,                                                                                                                                                                                                                                        
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.ADMIN_FUNCTION,                                                                                                                                                                                                                                        
       T.EFFDT,                                                                                                                                                                                                                                                 
       T.EFF_STATUS,                                                                                                                                                                                                                                            
       T.ADMIN_FUNCTION_SD,                                                                                                                                                                                                                                     
       T.ADMIN_FUNCTION_LD,                                                                                                                                                                                                                                     
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.ADMIN_FUNC_SID,                                                                                                                                                                                                                                        
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.ADMIN_FUNCTION,                                                                                                                                                                                                                                        
       S.EFFDT,                                                                                                                                                                                                                                                 
       S.EFF_STATUS,                                                                                                                                                                                                                                            
       S.ADMIN_FUNCTION_SD,                                                                                                                                                                                                                                     
       S.ADMIN_FUNCTION_LD,                                                                                                                                                                                                                                     
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;    

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ADMIN_FUNC rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ADMIN_FUNC',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_ADMIN_FUNC';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_ADMIN_FUNC';
update CSMRT_OWNER.PS_D_ADMIN_FUNC T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.ADMIN_FUNC_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_ADM_FUNCTN_TBL S
                    where T.ADMIN_FUNCTION = S.ADMIN_FUNCTION
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ADMIN_FUNC rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ADMIN_FUNC',
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

END PS_D_ADMIN_FUNC_P;
/
