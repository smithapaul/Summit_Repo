CREATE OR REPLACE PROCEDURE             "PS_D_EXT_TERM_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_EXT_TERM from PeopleSoft table PS_D_EXT_TERM.
--
 --V01  SMT-xxxx 11/03/2017,    James Doucette
--                              Converted from DataStage
--V02 2/12/2021              -- Srikanth,Pabbu made changes to EXT_TERM_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_EXT_TERM';
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

strMessage01    := 'Merging data into CSSTG_OWNER.PS_D_EXT_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.PS_D_EXT_TERM';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_EXT_TERM T                                                                                                                                                                                                 
using (                                                                                                                                                                                                                                                         
  with X1 as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as (  
select EXT_TERM_TYPE EXT_TERM_TYPE_ID, EXT_TERM EXT_TERM_ID, SRC_SYS_ID,  
       DESCRSHORT EXT_TERM_SD, DESCR EXT_TERM_LD, 
       DATA_ORIGIN  
  from CSSTG_OWNER.PS_EXT_TERM_TBL),
       S as (
select Q1.EXT_TERM_TYPE_ID, Q1.EXT_TERM_ID, Q1.SRC_SYS_ID,  
       nvl(X1.XLATSHORTNAME,'-') EXT_TERM_TYPE_SD, nvl(X1.XLATLONGNAME,'-') EXT_TERM_TYPE_LD, 
       Q1.EXT_TERM_SD, Q1.EXT_TERM_LD,   
       Q1.DATA_ORIGIN  
  from Q1
  left outer join X1
    on Q1.EXT_TERM_TYPE_ID = X1.FIELDVALUE
   and Q1.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'EXT_TERM_TYPE' 
   and X1.X_ORDER = 1)                                                                                                                                                                                               
select nvl(D.EXT_TERM_SID, --max(D.EXT_TERM_SID) over (partition by 1) +  This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021
  (select nvl(max(EXT_TERM_SID),0) from CSMRT_OWNER.PS_D_EXT_TERM where EXT_TERM_SID <> 2147483646) +                                                                                                                                                                                        
       row_number() over (partition by 1 order by D.EXT_TERM_SID nulls first)) EXT_TERM_SID,                                                                                                                                                                    
       nvl(D.EXT_TERM_TYPE_ID, S.EXT_TERM_TYPE_ID) EXT_TERM_TYPE_ID,                                                                                                                                                                                            
       nvl(D.EXT_TERM_ID, S.EXT_TERM_ID) EXT_TERM_ID,                                                                                                                                                                                                           
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.EXT_TERM_TYPE_SD, S.EXT_TERM_TYPE_SD, D.EXT_TERM_TYPE_SD, S.EXT_TERM_TYPE_SD) EXT_TERM_TYPE_SD,                                                                                                                                                 
       decode(D.EXT_TERM_TYPE_LD, S.EXT_TERM_TYPE_LD, D.EXT_TERM_TYPE_LD, S.EXT_TERM_TYPE_LD) EXT_TERM_TYPE_LD,                                                                                                                                                 
       decode(D.EXT_TERM_SD, S.EXT_TERM_SD, D.EXT_TERM_SD, S.EXT_TERM_SD) EXT_TERM_SD,                                                                                                                                                                          
       decode(D.EXT_TERM_LD, S.EXT_TERM_LD, D.EXT_TERM_LD, S.EXT_TERM_LD) EXT_TERM_LD,                                                                                                                                                                          
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_EXT_TERM D                                                                                                                                                                                                               
    on D.EXT_TERM_SID <> 2147483646                                                                                                                                                                                                                             
   and D.EXT_TERM_TYPE_ID = S.EXT_TERM_TYPE_ID                                                                                                                                                                                                                  
   and D.EXT_TERM_ID = S.EXT_TERM_ID                                                                                                                                                                                                                            
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.EXT_TERM_TYPE_ID = S.EXT_TERM_TYPE_ID                                                                                                                                                                                                                
   and  T.EXT_TERM_ID = S.EXT_TERM_ID                                                                                                                                                                                                                           
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EXT_TERM_TYPE_SD = S.EXT_TERM_TYPE_SD,                                                                                                                                                                                                                 
       T.EXT_TERM_TYPE_LD = S.EXT_TERM_TYPE_LD,                                                                                                                                                                                                                 
       T.EXT_TERM_SD = S.EXT_TERM_SD,                                                                                                                                                                                                                           
       T.EXT_TERM_LD = S.EXT_TERM_LD,                                                                                                                                                                                                                           
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EXT_TERM_TYPE_SD,S.EXT_TERM_TYPE_SD,0,1) = 1 or                                                                                                                                                                                                 
       decode(T.EXT_TERM_TYPE_LD,S.EXT_TERM_TYPE_LD,0,1) = 1 or                                                                                                                                                                                                 
       decode(T.EXT_TERM_SD,S.EXT_TERM_SD,0,1) = 1 or                                                                                                                                                                                                           
       decode(T.EXT_TERM_LD,S.EXT_TERM_LD,0,1) = 1 or                                                                                                                                                                                                           
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.EXT_TERM_SID,                                                                                                                                                                                                                                          
       T.EXT_TERM_TYPE_ID,                                                                                                                                                                                                                                      
       T.EXT_TERM_ID,                                                                                                                                                                                                                                           
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.EXT_TERM_TYPE_SD,                                                                                                                                                                                                                                      
       T.EXT_TERM_TYPE_LD,                                                                                                                                                                                                                                      
       T.EXT_TERM_SD,                                                                                                                                                                                                                                           
       T.EXT_TERM_LD,                                                                                                                                                                                                                                           
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.EXT_TERM_SID,                                                                                                                                                                                                                                          
       S.EXT_TERM_TYPE_ID,                                                                                                                                                                                                                                      
       S.EXT_TERM_ID,                                                                                                                                                                                                                                           
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.EXT_TERM_TYPE_SD,                                                                                                                                                                                                                                      
       S.EXT_TERM_TYPE_LD,                                                                                                                                                                                                                                      
       S.EXT_TERM_SD,                                                                                                                                                                                                                                           
       S.EXT_TERM_LD,                                                                                                                                                                                                                                           
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;                                                                                                                                                                                                                                                         

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_EXT_TERM rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_EXT_TERM',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.PS_D_EXT_TERM';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.PS_D_EXT_TERM';
update CSMRT_OWNER.PS_D_EXT_TERM T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.EXT_TERM_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_EXT_TERM_TBL S
                    where T.EXT_TERM_TYPE_ID = S.EXT_TERM_TYPE
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_EXT_TERM rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_EXT_TERM',
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

END PS_D_EXT_TERM_P;
/
