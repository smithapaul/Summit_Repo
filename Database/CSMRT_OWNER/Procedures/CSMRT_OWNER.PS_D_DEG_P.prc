DROP PROCEDURE CSMRT_OWNER.PS_D_DEG_P
/

--
-- PS_D_DEG_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_DEG_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_DEG from PeopleSoft table PS_DEGREE_TBL.
--
 --V01  SMT-xxxx 10/30/2017,    James Doucette
--                              Converted from DataStage
--V02 2/12/2021            -- Srikanth,Pabbu made changes to DEG_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_DEG';
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


strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_DEG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_DEG';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_DEG T                                                                                                                                                                                                      
using (                                                                                                                                                                                                                                                         
  with X1 as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as (  
select DEGREE DEG_CD, SRC_SYS_ID, EFFDT, EFF_STATUS EFF_STAT_CD, 
       DESCRSHORT DEG_SD, DESCR DEG_LD, DESCRFORMAL DEG_FD, EDUCATION_LVL EDU_LVL_CD, 
       INTERNAL_DEGREE INTERNAL_DEG_FLG, YEARS_OF_EDUCATN YRS_OF_EDU_NUM, 
       DATA_ORIGIN,  
       row_number() over (partition by DEGREE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q1_ORDER
  from CSSTG_OWNER.PS_DEGREE_TBL),
       S as (
select Q1.DEG_CD, Q1.SRC_SYS_ID, Q1.EFFDT, Q1.EFF_STAT_CD, 
       Q1.DEG_SD, Q1.DEG_LD, Q1.DEG_FD, Q1.EDU_LVL_CD, nvl(X1.XLATSHORTNAME,'-') EDU_LVL_SD, nvl(X1.XLATLONGNAME,'-') EDU_LVL_LD, Q1.INTERNAL_DEG_FLG, Q1.YRS_OF_EDU_NUM, 
       Q1.DATA_ORIGIN  
  from Q1
  left outer join X1
    on Q1.EDU_LVL_CD = X1.FIELDVALUE
   and X1.FIELDNAME = 'EDUCATION_LVL'
   and X1.X_ORDER = 1  
 where Q1_ORDER = 1)                                                                                                                                                                                              
select nvl(D.DEG_SID, --max(D.DEG_SID) over (partition by 1) +  This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021
 (select nvl(max(DEG_SID),0) from CSMRT_OWNER.PS_D_DEG where DEG_SID <> 2147483646) +                                                                                                                                                                                                  
       row_number() over (partition by 1 order by D.DEG_SID nulls first)) DEG_SID,                                                                                                                                                                              
       nvl(D.DEG_CD, S.DEG_CD) DEG_CD,                                                                                                                                                                                                                          
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,                                                                                                                                                                                                        
       decode(D.EFF_STAT_CD, S.EFF_STAT_CD, D.EFF_STAT_CD, S.EFF_STAT_CD) EFF_STAT_CD,                                                                                                                                                                          
       decode(D.DEG_SD, S.DEG_SD, D.DEG_SD, S.DEG_SD) DEG_SD,                                                                                                                                                                                                   
       decode(D.DEG_LD, S.DEG_LD, D.DEG_LD, S.DEG_LD) DEG_LD,                                                                                                                                                                                                   
       decode(D.DEG_FD, S.DEG_FD, D.DEG_FD, S.DEG_FD) DEG_FD,                                                                                                                                                                                                   
       decode(D.EDU_LVL_CD, S.EDU_LVL_CD, D.EDU_LVL_CD, S.EDU_LVL_CD) EDU_LVL_CD,                                                                                                                                                                               
       decode(D.EDU_LVL_SD, S.EDU_LVL_SD, D.EDU_LVL_SD, S.EDU_LVL_SD) EDU_LVL_SD,                                                                                                                                                                               
       decode(D.EDU_LVL_LD, S.EDU_LVL_LD, D.EDU_LVL_LD, S.EDU_LVL_LD) EDU_LVL_LD,                                                                                                                                                                               
       decode(D.INTERNAL_DEG_FLG, S.INTERNAL_DEG_FLG, D.INTERNAL_DEG_FLG, S.INTERNAL_DEG_FLG) INTERNAL_DEG_FLG,                                                                                                                                                 
       decode(D.YRS_OF_EDU_NUM, S.YRS_OF_EDU_NUM, D.YRS_OF_EDU_NUM, S.YRS_OF_EDU_NUM) YRS_OF_EDU_NUM,                                                                                                                                                           
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_DEG D                                                                                                                                                                                                                    
    on D.DEG_SID <> 2147483646                                                                                                                                                                                                                                  
   and D.DEG_CD = S.DEG_CD                                                                                                                                                                                                                                      
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.DEG_CD = S.DEG_CD                                                                                                                                                                                                                                    
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                       
       T.EFF_STAT_CD = S.EFF_STAT_CD,                                                                                                                                                                                                                           
       T.DEG_SD = S.DEG_SD,                                                                                                                                                                                                                                     
       T.DEG_LD = S.DEG_LD,                                                                                                                                                                                                                                     
       T.DEG_FD = S.DEG_FD,                                                                                                                                                                                                                                     
       T.EDU_LVL_CD = S.EDU_LVL_CD,                                                                                                                                                                                                                             
       T.EDU_LVL_SD = S.EDU_LVL_SD,                                                                                                                                                                                                                             
       T.EDU_LVL_LD = S.EDU_LVL_LD,                                                                                                                                                                                                                             
       T.INTERNAL_DEG_FLG = S.INTERNAL_DEG_FLG,                                                                                                                                                                                                                 
       T.YRS_OF_EDU_NUM = S.YRS_OF_EDU_NUM,                                                                                                                                                                                                                     
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                       
       decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or                                                                                                                                                                                                           
       decode(T.DEG_SD,S.DEG_SD,0,1) = 1 or                                                                                                                                                                                                                     
       decode(T.DEG_LD,S.DEG_LD,0,1) = 1 or                                                                                                                                                                                                                     
       decode(T.DEG_FD,S.DEG_FD,0,1) = 1 or                                                                                                                                                                                                                     
       decode(T.EDU_LVL_CD,S.EDU_LVL_CD,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.EDU_LVL_SD,S.EDU_LVL_SD,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.EDU_LVL_LD,S.EDU_LVL_LD,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.INTERNAL_DEG_FLG,S.INTERNAL_DEG_FLG,0,1) = 1 or                                                                                                                                                                                                 
       decode(T.YRS_OF_EDU_NUM,S.YRS_OF_EDU_NUM,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.DEG_SID,                                                                                                                                                                                                                                               
       T.DEG_CD,                                                                                                                                                                                                                                                
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.EFFDT,                                                                                                                                                                                                                                                 
       T.EFF_STAT_CD,                                                                                                                                                                                                                                           
       T.DEG_SD,                                                                                                                                                                                                                                                
       T.DEG_LD,                                                                                                                                                                                                                                                
       T.DEG_FD,                                                                                                                                                                                                                                                
       T.EDU_LVL_CD,                                                                                                                                                                                                                                            
       T.EDU_LVL_SD,                                                                                                                                                                                                                                            
       T.EDU_LVL_LD,                                                                                                                                                                                                                                            
       T.INTERNAL_DEG_FLG,                                                                                                                                                                                                                                      
       T.YRS_OF_EDU_NUM,                                                                                                                                                                                                                                        
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.DEG_SID,                                                                                                                                                                                                                                               
       S.DEG_CD,                                                                                                                                                                                                                                                
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.EFFDT,                                                                                                                                                                                                                                                 
       S.EFF_STAT_CD,                                                                                                                                                                                                                                           
       S.DEG_SD,                                                                                                                                                                                                                                                
       S.DEG_LD,                                                                                                                                                                                                                                                
       S.DEG_FD,                                                                                                                                                                                                                                                
       S.EDU_LVL_CD,                                                                                                                                                                                                                                            
       S.EDU_LVL_SD,                                                                                                                                                                                                                                            
       S.EDU_LVL_LD,                                                                                                                                                                                                                                            
       S.INTERNAL_DEG_FLG,                                                                                                                                                                                                                                      
       S.YRS_OF_EDU_NUM,                                                                                                                                                                                                                                        
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;                                                                                                                                                                                                                                                

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_DEG rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_DEG',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_DEG';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_DEG';
update CSMRT_OWNER.PS_D_DEG T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.DEG_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_DEGREE_TBL S 
                    where T.DEG_CD = S.DEGREE
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_DEG rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_DEG',
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

END PS_D_DEG_P;
/
