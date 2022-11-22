DROP PROCEDURE CSMRT_OWNER.PS_D_EVAL_STATUS_P
/

--
-- PS_D_EVAL_STATUS_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_EVAL_STATUS_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_EVAL_STATUS from PeopleSoft table PS_D_EVAL_STATUS.
--
 --V01  SMT-xxxx 11/01/2017,    James Doucette
--                              Converted from DataStage
--V02 2/12/2021              -- Srikanth,Pabbu made changes to EVAL_STAT_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_EVAL_STATUS';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_EVAL_STATUS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_EVAL_STATUS';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_EVAL_STATUS T                                                                                                                                                                                              
using (                                                                                                                                                                                                                                                         
  with Q1 as (  
select INSTITUTION INSTITUTION_CD, EVALUATN_STATUS EVAL_STATUS_CD, SRC_SYS_ID, EFFDT, EFF_STATUS EFF_STATUS_CD, 
       DESCRSHORT EVAL_STATUS_SD, DESCR EVAL_STATUS_LD, EVAL_IN_PROGRESS EVAL_IN_PROG_FLG, 
       DATA_ORIGIN,  
       row_number() over (partition by INSTITUTION, EVALUATN_STATUS, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_EVAL_STATUS_TBL),
       S as (
select INSTITUTION_CD, EVAL_STATUS_CD, SRC_SYS_ID, EFFDT, EFF_STATUS_CD, 
       EVAL_STATUS_SD, EVAL_STATUS_LD, EVAL_IN_PROG_FLG, 
       DATA_ORIGIN  
  from Q1
 where Q1.Q_ORDER = 1)                                                                                                                                                                                              
select nvl(D.EVAL_STAT_SID, --max(D.EVAL_STAT_SID) over (partition by 1) +  This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021   
 (select nvl(max(EVAL_STAT_SID),0) from CSMRT_OWNER.PS_D_EVAL_STATUS where EVAL_STAT_SID <> 2147483646) +                                                                                                                                                                                    
       row_number() over (partition by 1 order by D.EVAL_STAT_SID nulls first)) EVAL_STAT_SID,                                                                                                                                                                  
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD,                                                                                                                                                                                                  
       nvl(D.EVAL_STATUS_CD, S.EVAL_STATUS_CD) EVAL_STATUS_CD,                                                                                                                                                                                                  
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,                                                                                                                                                                                                        
       decode(D.EFF_STATUS_CD, S.EFF_STATUS_CD, D.EFF_STATUS_CD, S.EFF_STATUS_CD) EFF_STATUS_CD,                                                                                                                                                                
       decode(D.EVAL_STATUS_SD, S.EVAL_STATUS_SD, D.EVAL_STATUS_SD, S.EVAL_STATUS_SD) EVAL_STATUS_SD,                                                                                                                                                           
       decode(D.EVAL_STATUS_LD, S.EVAL_STATUS_LD, D.EVAL_STATUS_LD, S.EVAL_STATUS_LD) EVAL_STATUS_LD,                                                                                                                                                           
       decode(D.EVAL_IN_PROG_FLG, S.EVAL_IN_PROG_FLG, D.EVAL_IN_PROG_FLG, S.EVAL_IN_PROG_FLG) EVAL_IN_PROG_FLG,                                                                                                                                                 
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_EVAL_STATUS D                                                                                                                                                                                                            
    on D.EVAL_STAT_SID <> 2147483646                                                                                                                                                                                                                            
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
   and D.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                      
   and D.EVAL_STATUS_CD = S.EVAL_STATUS_CD                                                                                                                                                                                                                      
) S                                                                                                                                                                                                                                                             
    on  (T.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                    
   and  T.EVAL_STATUS_CD = S.EVAL_STATUS_CD                                                                                                                                                                                                                     
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                       
       T.EFF_STATUS_CD = S.EFF_STATUS_CD,                                                                                                                                                                                                                       
       T.EVAL_STATUS_SD = S.EVAL_STATUS_SD,                                                                                                                                                                                                                     
       T.EVAL_STATUS_LD = S.EVAL_STATUS_LD,                                                                                                                                                                                                                     
       T.EVAL_IN_PROG_FLG = S.EVAL_IN_PROG_FLG,                                                                                                                                                                                                                 
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                       
       decode(T.EFF_STATUS_CD,S.EFF_STATUS_CD,0,1) = 1 or                                                                                                                                                                                                       
       decode(T.EVAL_STATUS_SD,S.EVAL_STATUS_SD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.EVAL_STATUS_LD,S.EVAL_STATUS_LD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.EVAL_IN_PROG_FLG,S.EVAL_IN_PROG_FLG,0,1) = 1 or                                                                                                                                                                                                 
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.EVAL_STAT_SID,                                                                                                                                                                                                                                         
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.INSTITUTION_CD,                                                                                                                                                                                                                                        
       T.EVAL_STATUS_CD,                                                                                                                                                                                                                                        
       T.EFFDT,                                                                                                                                                                                                                                                 
       T.EFF_STATUS_CD,                                                                                                                                                                                                                                         
       T.EVAL_STATUS_SD,                                                                                                                                                                                                                                        
       T.EVAL_STATUS_LD,                                                                                                                                                                                                                                        
       T.EVAL_IN_PROG_FLG,                                                                                                                                                                                                                                      
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.EVAL_STAT_SID,                                                                                                                                                                                                                                         
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.INSTITUTION_CD,                                                                                                                                                                                                                                        
       S.EVAL_STATUS_CD,                                                                                                                                                                                                                                        
       S.EFFDT,                                                                                                                                                                                                                                                 
       S.EFF_STATUS_CD,                                                                                                                                                                                                                                         
       S.EVAL_STATUS_SD,                                                                                                                                                                                                                                        
       S.EVAL_STATUS_LD,                                                                                                                                                                                                                                        
       S.EVAL_IN_PROG_FLG,                                                                                                                                                                                                                                      
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;       

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_EVAL_STATUS rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_EVAL_STATUS',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_EVAL_STATUS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_EVAL_STATUS';
update CSMRT_OWNER.PS_D_EVAL_STATUS T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.EVAL_STAT_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_EVAL_STATUS_TBL S
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.EVAL_STATUS_CD = S.EVALUATN_STATUS
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_EVAL_STATUS rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_EVAL_STATUS',
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

END PS_D_EVAL_STATUS_P;
/
