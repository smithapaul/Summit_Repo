DROP PROCEDURE CSMRT_OWNER.PS_D_APPL_MTHD_P
/

--
-- PS_D_APPL_MTHD_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_APPL_MTHD_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_APPL_MTHD from PeopleSoft table PS_D_APPL_MTHD.
--
 --V01  SMT-xxxx 10/27/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_APPL_MTHD';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_APPL_MTHD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_APPL_MTHD';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_APPL_MTHD T                                                                                                                                                                                                
using (                                                                                                                                                                                                                                                         
  with X as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) XLAT_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where FIELDNAME = 'ADM_APPL_METHOD' 
),
       S as (
select FIELDVALUE APPL_MTHD_ID, SRC_SYS_ID, 
       XLATSHORTNAME APPL_MTHD_SD, XLATLONGNAME APPL_MTHD_LD, DATA_ORIGIN 
  from X 
 where XLAT_ORDER = 1)                                                                                                                                                                                             
--select nvl(D.APPL_MTHD_SID, max(D.APPL_MTHD_SID) over (partition by 1) +                                                                                                                                                                                        
--       row_number() over (partition by 1 order by D.APPL_MTHD_SID nulls first)) APPL_MTHD_SID,                                                                                                                                                                  
select nvl(D.APPL_MTHD_SID,
(select nvl(max(APPL_MTHD_SID),0) from CSMRT_OWNER.PS_D_APPL_MTHD where APPL_MTHD_SID <> 2147483646) +
row_number() over (partition by 1 order by D.APPL_MTHD_SID nulls first)) APPL_MTHD_SID, -- December 2020                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       nvl(D.APPL_MTHD_ID, S.APPL_MTHD_ID) APPL_MTHD_ID,                                                                                                                                                                                                        
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.APPL_MTHD_SD, S.APPL_MTHD_SD, D.APPL_MTHD_SD, S.APPL_MTHD_SD) APPL_MTHD_SD,                                                                                                                                                                     
       decode(D.APPL_MTHD_LD, S.APPL_MTHD_LD, D.APPL_MTHD_LD, S.APPL_MTHD_LD) APPL_MTHD_LD,                                                                                                                                                                     
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_APPL_MTHD D                                                                                                                                                                                                              
    on D.APPL_MTHD_SID <> 2147483646                                                                                                                                                                                                                            
   and D.APPL_MTHD_ID = S.APPL_MTHD_ID                                                                                                                                                                                                                          
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.APPL_MTHD_ID = S.APPL_MTHD_ID                                                                                                                                                                                                                        
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.APPL_MTHD_SD = S.APPL_MTHD_SD,                                                                                                                                                                                                                         
       T.APPL_MTHD_LD = S.APPL_MTHD_LD,                                                                                                                                                                                                                         
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.APPL_MTHD_SD,S.APPL_MTHD_SD,0,1) = 1 or                                                                                                                                                                                                         
       decode(T.APPL_MTHD_LD,S.APPL_MTHD_LD,0,1) = 1 or                                                                                                                                                                                                         
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.APPL_MTHD_SID,                                                                                                                                                                                                                                         
       T.APPL_MTHD_ID,                                                                                                                                                                                                                                          
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.APPL_MTHD_SD,                                                                                                                                                                                                                                          
       T.APPL_MTHD_LD,                                                                                                                                                                                                                                          
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.APPL_MTHD_SID,                                                                                                                                                                                                                                         
       S.APPL_MTHD_ID,                                                                                                                                                                                                                                          
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.APPL_MTHD_SD,                                                                                                                                                                                                                                          
       S.APPL_MTHD_LD,                                                                                                                                                                                                                                          
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;                                                                                                                                                                                                                                               

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_APPL_MTHD rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_APPL_MTHD',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_APPL_MTHD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_APPL_MTHD';
update CSMRT_OWNER.PS_D_APPL_MTHD T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.APPL_MTHD_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PSXLATITEM S
                    where S.FIELDNAME = 'ADM_APPL_METHOD'
                      and T.APPL_MTHD_ID = S.FIELDVALUE
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_APPL_MTHD rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_APPL_MTHD',
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

END PS_D_APPL_MTHD_P;
/
