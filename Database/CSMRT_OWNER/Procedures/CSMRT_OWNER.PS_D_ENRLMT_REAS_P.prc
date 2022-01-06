CREATE OR REPLACE PROCEDURE             "PS_D_ENRLMT_REAS_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_ENRLMT_REAS from PeopleSoft table PS_D_ENRLMT_REAS.
--
 --V01  SMT-xxxx 11/01/2017,    James Doucette
--                              Converted from DataStage
--V02 2/12/2021              -- Srikanth,Pabbu made changes to ENRLMT_REAS_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_ENRLMT_REAS';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_ENRLMT_REAS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_ENRLMT_REAS';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_ENRLMT_REAS T                                                                                                                                                                                              
using (                                                                                                                                                                                                                                                         
  with X as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where FIELDNAME = 'ENRL_STATUS_REASON' 
),
       S as (
select FIELDVALUE ENRLMT_REAS_ID, SRC_SYS_ID, 
       XLATSHORTNAME ENRLMT_REAS_SD, XLATLONGNAME ENRLMT_REAS_LD, DATA_ORIGIN 
  from X 
 where X_ORDER = 1)                                                                                                                                                                                              
select nvl(D.ENRLMT_REAS_SID, --max(D.ENRLMT_REAS_SID) over (partition by 1) +  This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021
(select nvl(max(ENRLMT_REAS_SID),0) from CSMRT_OWNER.PS_D_ENRLMT_REAS where ENRLMT_REAS_SID <> 2147483646) +                                                                                                                                                                                   
       row_number() over (partition by 1 order by D.ENRLMT_REAS_SID nulls first)) ENRLMT_REAS_SID,                                                                                                                                                              
       nvl(D.ENRLMT_REAS_ID, S.ENRLMT_REAS_ID) ENRLMT_REAS_ID,                                                                                                                                                                                                  
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.ENRLMT_REAS_SD, S.ENRLMT_REAS_SD, D.ENRLMT_REAS_SD, S.ENRLMT_REAS_SD) ENRLMT_REAS_SD,                                                                                                                                                           
       decode(D.ENRLMT_REAS_LD, S.ENRLMT_REAS_LD, D.ENRLMT_REAS_LD, S.ENRLMT_REAS_LD) ENRLMT_REAS_LD,                                                                                                                                                           
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_ENRLMT_REAS D                                                                                                                                                                                                            
    on D.ENRLMT_REAS_SID <> 2147483646                                                                                                                                                                                                                          
   and D.ENRLMT_REAS_ID = S.ENRLMT_REAS_ID                                                                                                                                                                                                                      
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.ENRLMT_REAS_ID = S.ENRLMT_REAS_ID                                                                                                                                                                                                                    
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.ENRLMT_REAS_SD = S.ENRLMT_REAS_SD,                                                                                                                                                                                                                     
       T.ENRLMT_REAS_LD = S.ENRLMT_REAS_LD,                                                                                                                                                                                                                     
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.ENRLMT_REAS_SD,S.ENRLMT_REAS_SD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.ENRLMT_REAS_LD,S.ENRLMT_REAS_LD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.ENRLMT_REAS_SID,                                                                                                                                                                                                                                       
       T.ENRLMT_REAS_ID,                                                                                                                                                                                                                                        
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.ENRLMT_REAS_SD,                                                                                                                                                                                                                                        
       T.ENRLMT_REAS_LD,                                                                                                                                                                                                                                        
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.ENRLMT_REAS_SID,                                                                                                                                                                                                                                       
       S.ENRLMT_REAS_ID,                                                                                                                                                                                                                                        
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.ENRLMT_REAS_SD,                                                                                                                                                                                                                                        
       S.ENRLMT_REAS_LD,                                                                                                                                                                                                                                        
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;  

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ENRLMT_REAS rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ENRLMT_REAS',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_ENRLMT_REAS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_ENRLMT_REAS';
update CSMRT_OWNER.PS_D_ENRLMT_REAS T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.ENRLMT_REAS_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PSXLATITEM S
                    where S.FIELDNAME = 'ENRL_STATUS_REASON'
                      and S.FIELDVALUE = T.ENRLMT_REAS_ID 
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ENRLMT_REAS rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ENRLMT_REAS',
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

END PS_D_ENRLMT_REAS_P;
/
