CREATE OR REPLACE PROCEDURE             "PS_D_INSTRCTR_ROLE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_INSTRCTR_ROLE from PeopleSoft table PS_D_INSTRCTR_ROLE.
--
 --V01  SMT-xxxx 11/16/2017,    James Doucette
--                              Converted from DataStage
--V02 2/12/2021               --Srikanth,Pabbu made changes to INSTRCTR_ROLE_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_INSTRCTR_ROLE';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_INSTRCTR_ROLE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_INSTRCTR_ROLE';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_INSTRCTR_ROLE T                                                                                                                                                                                            
using (                                                                                                                                                                                                                                                         
  with X as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where FIELDNAME = 'INSTR_ROLE' 
),
       S as (
select FIELDVALUE INSTRCTR_ROLE_CD, SRC_SYS_ID, 
       XLATSHORTNAME INSTRCTR_ROLE_SD, XLATLONGNAME INSTRCTR_ROLE_LD, DATA_ORIGIN 
  from X 
 where X_ORDER = 1)                                                                                                                                                                                              
select nvl(D.INSTRCTR_ROLE_SID, --max(D.INSTRCTR_ROLE_SID) over (partition by 1) + This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021
  (select nvl(max(INSTRCTR_ROLE_SID),0) from CSMRT_OWNER.PS_D_INSTRCTR_ROLE where INSTRCTR_ROLE_SID <> 2147483646) +                                                                                                                                                                              
       row_number() over (partition by 1 order by D.INSTRCTR_ROLE_SID nulls first)) INSTRCTR_ROLE_SID,                                                                                                                                                          
       nvl(D.INSTRCTR_ROLE_CD, S.INSTRCTR_ROLE_CD) INSTRCTR_ROLE_CD,                                                                                                                                                                                            
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.INSTRCTR_ROLE_SD, S.INSTRCTR_ROLE_SD, D.INSTRCTR_ROLE_SD, S.INSTRCTR_ROLE_SD) INSTRCTR_ROLE_SD,                                                                                                                                                 
       decode(D.INSTRCTR_ROLE_LD, S.INSTRCTR_ROLE_LD, D.INSTRCTR_ROLE_LD, S.INSTRCTR_ROLE_LD) INSTRCTR_ROLE_LD,                                                                                                                                                 
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_INSTRCTR_ROLE D                                                                                                                                                                                                          
    on D.INSTRCTR_ROLE_SID <> 2147483646                                                                                                                                                                                                                        
   and D.INSTRCTR_ROLE_CD = S.INSTRCTR_ROLE_CD                                                                                                                                                                                                                  
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.INSTRCTR_ROLE_CD = S.INSTRCTR_ROLE_CD                                                                                                                                                                                                                
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.INSTRCTR_ROLE_SD = S.INSTRCTR_ROLE_SD,                                                                                                                                                                                                                 
       T.INSTRCTR_ROLE_LD = S.INSTRCTR_ROLE_LD,                                                                                                                                                                                                                 
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.INSTRCTR_ROLE_SD,S.INSTRCTR_ROLE_SD,0,1) = 1 or                                                                                                                                                                                                 
       decode(T.INSTRCTR_ROLE_LD,S.INSTRCTR_ROLE_LD,0,1) = 1 or                                                                                                                                                                                                 
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.INSTRCTR_ROLE_SID,                                                                                                                                                                                                                                     
       T.INSTRCTR_ROLE_CD,                                                                                                                                                                                                                                      
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.INSTRCTR_ROLE_SD,                                                                                                                                                                                                                                      
       T.INSTRCTR_ROLE_LD,                                                                                                                                                                                                                                      
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.INSTRCTR_ROLE_SID,                                                                                                                                                                                                                                     
       S.INSTRCTR_ROLE_CD,                                                                                                                                                                                                                                      
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.INSTRCTR_ROLE_SD,                                                                                                                                                                                                                                      
       S.INSTRCTR_ROLE_LD,                                                                                                                                                                                                                                      
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_INSTRCTR_ROLE rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_INSTRCTR_ROLE',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_INSTRCTR_ROLE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_INSTRCTR_ROLE';
update CSMRT_OWNER.PS_D_INSTRCTR_ROLE T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.INSTRCTR_ROLE_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PSXLATITEM S
                    where S.FIELDNAME = 'INSTR_ROLE'
                      and T.INSTRCTR_ROLE_CD = S.FIELDVALUE
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_INSTRCTR_ROLE rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_INSTRCTR_ROLE',
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

END PS_D_INSTRCTR_ROLE_P;
/
