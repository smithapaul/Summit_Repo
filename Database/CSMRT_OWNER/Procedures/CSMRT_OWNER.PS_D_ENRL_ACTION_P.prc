CREATE OR REPLACE PROCEDURE             "PS_D_ENRL_ACTION_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_ENRL_ACTION from PeopleSoft table PS_D_ENRL_ACTION.
--
 --V01  SMT-xxxx 11/01/2017,    James Doucette
--                              Converted from DataStage
--V02 2/12/2021              -- Srikanth,Pabbu made changes to ENRL_REQ_ACTN_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_ENRL_ACTION';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_ENRL_ACTION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_ENRL_ACTION';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_ENRL_ACTION T                                                                                                                                                                                              
using (                                                                                                                                                                                                                                                         
  with X as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where FIELDNAME = 'ENRL_REQ_ACTION' 
),
       S as (
select FIELDVALUE ENRL_REQ_ACTION, SRC_SYS_ID, 
       XLATSHORTNAME ENRL_REQ_ACTION_SD, XLATLONGNAME ENRL_REQ_ACTION_LD, DATA_ORIGIN 
  from X 
 where X_ORDER = 1)                                                                                                                                                                                              
select nvl(D.ENRL_REQ_ACTN_SID, --max(D.ENRL_REQ_ACTN_SID) over (partition by 1) +  This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021 
 (select nvl(max(ENRL_REQ_ACTN_SID),0) from CSMRT_OWNER.PS_D_ENRL_ACTION where ENRL_REQ_ACTN_SID <> 2147483646) +                                                                                                                                                                            
       row_number() over (partition by 1 order by D.ENRL_REQ_ACTN_SID nulls first)) ENRL_REQ_ACTN_SID,                                                                                                                                                          
       nvl(D.ENRL_REQ_ACTION, S.ENRL_REQ_ACTION) ENRL_REQ_ACTION,                                                                                                                                                                                               
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.ENRL_REQ_ACTION_SD, S.ENRL_REQ_ACTION_SD, D.ENRL_REQ_ACTION_SD, S.ENRL_REQ_ACTION_SD) ENRL_REQ_ACTION_SD,                                                                                                                                       
       decode(D.ENRL_REQ_ACTION_LD, S.ENRL_REQ_ACTION_LD, D.ENRL_REQ_ACTION_LD, S.ENRL_REQ_ACTION_LD) ENRL_REQ_ACTION_LD,                                                                                                                                       
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_ENRL_ACTION D                                                                                                                                                                                                            
    on D.ENRL_REQ_ACTN_SID <> 2147483646                                                                                                                                                                                                                        
   and D.ENRL_REQ_ACTION = S.ENRL_REQ_ACTION                                                                                                                                                                                                                    
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.ENRL_REQ_ACTION = S.ENRL_REQ_ACTION                                                                                                                                                                                                                  
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.ENRL_REQ_ACTION_SD = S.ENRL_REQ_ACTION_SD,                                                                                                                                                                                                             
       T.ENRL_REQ_ACTION_LD = S.ENRL_REQ_ACTION_LD,                                                                                                                                                                                                             
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.ENRL_REQ_ACTION_SD,S.ENRL_REQ_ACTION_SD,0,1) = 1 or                                                                                                                                                                                             
       decode(T.ENRL_REQ_ACTION_LD,S.ENRL_REQ_ACTION_LD,0,1) = 1 or                                                                                                                                                                                             
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.ENRL_REQ_ACTN_SID,                                                                                                                                                                                                                                     
       T.ENRL_REQ_ACTION,                                                                                                                                                                                                                                       
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.ENRL_REQ_ACTION_SD,                                                                                                                                                                                                                                    
       T.ENRL_REQ_ACTION_LD,                                                                                                                                                                                                                                    
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.ENRL_REQ_ACTN_SID,                                                                                                                                                                                                                                     
       S.ENRL_REQ_ACTION,                                                                                                                                                                                                                                       
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.ENRL_REQ_ACTION_SD,                                                                                                                                                                                                                                    
       S.ENRL_REQ_ACTION_LD,                                                                                                                                                                                                                                    
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;   

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ENRL_ACTION rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ENRL_ACTION',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_ENRL_ACTION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_ENRL_ACTION';
update CSMRT_OWNER.PS_D_ENRL_ACTION T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.ENRL_REQ_ACTN_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PSXLATITEM S
                    where S.FIELDNAME = 'ENRL_REQ_ACTION'
                      and S.FIELDVALUE = T.ENRL_REQ_ACTION 
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ENRL_ACTION rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ENRL_ACTION',
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

END PS_D_ENRL_ACTION_P;
/
