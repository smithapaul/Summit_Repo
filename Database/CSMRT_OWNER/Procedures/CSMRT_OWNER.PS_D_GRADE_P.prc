DROP PROCEDURE CSMRT_OWNER.PS_D_GRADE_P
/

--
-- PS_D_GRADE_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_GRADE_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_GRADE from PeopleSoft table PS_D_GRADE.
--
-- V01   SMT-xxxx 11/08/2017,    James Doucette
--                               Converted from DataStage
-- V01.1 SMT-xxxx 09/05/2018,    James Doucette
--                               fixed Null value issue.
--V02 2/12/2021               -- Srikanth,Pabbu made changes to GRADE_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_GRADE';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_GRADE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_GRADE';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_GRADE T                                                                                                                                                                                                    
using (                                                                                                                                                                                                                                                         
  with X as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as ( 
select SETID, GRADING_SCHEME GRADE_SCHEME_CD, GRADING_BASIS GRADE_BASIS_CD, CRSE_GRADE_INPUT GRADE_CD, SRC_SYS_ID, EFFDT,  
       DESCRSHORT GRADE_SD, DESCR GRADE_LD,
       GRADE_CATEGORY GRADE_CTGRY_CD, GRADE_POINTS GRADE_PTS, EARN_CREDIT ERN_CRED_FLG, INCLUDE_IN_GPA INCLUDE_GPA_FLG, 
       DATA_ORIGIN,  
       row_number() over (partition by SETID, GRADING_SCHEME, GRADING_BASIS, CRSE_GRADE_INPUT, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_GRADE_TBL),
       Q2 as (
select SETID, GRADING_SCHEME GRADE_SCHEME_CD, SRC_SYS_ID, EFFDT, 
       DESCRSHORT GRADE_SCHEME_SD, DESCR GRADE_SCHEME_LD,  
       row_number() over (partition by SETID, GRADING_SCHEME, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_GRADESCHEME_TBL
 where DATA_ORIGIN <> 'D'),
       S as (
select Q1.SETID, Q1.GRADE_SCHEME_CD, Q1.GRADE_BASIS_CD, Q1.GRADE_CD, Q1.SRC_SYS_ID, Q1.EFFDT,  
       Q1.GRADE_SD, Q1.GRADE_LD, 
       nvl(Q2.GRADE_SCHEME_SD, '-') GRADE_SCHEME_SD, nvl(Q2.GRADE_SCHEME_LD, '-') GRADE_SCHEME_LD,
       nvl(X1.XLATSHORTNAME,'-') GRADE_BASIS_SD, nvl(X1.XLATLONGNAME,'-') GRADE_BASIS_LD,
       Q1.GRADE_CTGRY_CD, nvl(X2.XLATSHORTNAME,'-') GRADE_CTGRY_SD, nvl(X2.XLATLONGNAME,'-') GRADE_CTGRY_LD,
       Q1.ERN_CRED_FLG, Q1.INCLUDE_GPA_FLG, Q1.GRADE_PTS, 
       Q1.DATA_ORIGIN  
  from Q1
  left outer join Q2
    on Q1.SETID = Q2.SETID
   and Q1.GRADE_SCHEME_CD = Q2.GRADE_SCHEME_CD
   and Q1.SRC_SYS_ID = Q2.SRC_SYS_ID
   and Q2.Q_ORDER = 1
  left outer join X X1
    on Q1.GRADE_BASIS_CD = X1.FIELDVALUE
   and Q1.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'GRADING_BASIS' 
   and X1.X_ORDER = 1  
  left outer join X X2
    on Q1.GRADE_CTGRY_CD = X2.FIELDVALUE
   and Q1.SRC_SYS_ID = X2.SRC_SYS_ID
   and X2.FIELDNAME = 'GRADE_CATEGORY' 
   and X2.X_ORDER = 1  
 where Q1.Q_ORDER = 1)                                                                                                                                                                                              
select nvl(D.GRADE_SID, --max(D.GRADE_SID) over (partition by 1) + This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021
 (select nvl(max(GRADE_SID),0) from CSMRT_OWNER.PS_D_GRADE where GRADE_SID <> 2147483646) +                                                                                                                                                                                                
       row_number() over (partition by 1 order by D.GRADE_SID nulls first)) GRADE_SID,                                                                                                                                                                          
       nvl(D.SETID, S.SETID) SETID,                                                                                                                                                                                                                             
       nvl(D.GRADE_SCHEME_CD, S.GRADE_SCHEME_CD) GRADE_SCHEME_CD,                                                                                                                                                                                               
       nvl(D.GRADE_BASIS_CD, S.GRADE_BASIS_CD) GRADE_BASIS_CD,                                                                                                                                                                                                  
       nvl(D.GRADE_CD, S.GRADE_CD) GRADE_CD,                                                                                                                                                                                                                    
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,                                                                                                                                                                                                        
       decode(D.GRADE_SD, S.GRADE_SD, D.GRADE_SD, S.GRADE_SD) GRADE_SD,                                                                                                                                                                                         
       decode(D.GRADE_LD, S.GRADE_LD, D.GRADE_LD, S.GRADE_LD) GRADE_LD,                                                                                                                                                                                         
       decode(D.GRADE_SCHEME_SD, S.GRADE_SCHEME_SD, D.GRADE_SCHEME_SD, S.GRADE_SCHEME_SD) GRADE_SCHEME_SD,                                                                                                                                                      
       decode(D.GRADE_SCHEME_LD, S.GRADE_SCHEME_LD, D.GRADE_SCHEME_LD, S.GRADE_SCHEME_LD) GRADE_SCHEME_LD,                                                                                                                                                      
       decode(D.GRADE_BASIS_SD, S.GRADE_BASIS_SD, D.GRADE_BASIS_SD, S.GRADE_BASIS_SD) GRADE_BASIS_SD,                                                                                                                                                           
       decode(D.GRADE_BASIS_LD, S.GRADE_BASIS_LD, D.GRADE_BASIS_LD, S.GRADE_BASIS_LD) GRADE_BASIS_LD,                                                                                                                                                           
       decode(D.GRADE_CTGRY_CD, S.GRADE_CTGRY_CD, D.GRADE_CTGRY_CD, S.GRADE_CTGRY_CD) GRADE_CTGRY_CD,                                                                                                                                                           
       decode(D.GRADE_CTGRY_SD, S.GRADE_CTGRY_SD, D.GRADE_CTGRY_SD, S.GRADE_CTGRY_SD) GRADE_CTGRY_SD,                                                                                                                                                           
       decode(D.GRADE_CTGRY_LD, S.GRADE_CTGRY_LD, D.GRADE_CTGRY_LD, S.GRADE_CTGRY_LD) GRADE_CTGRY_LD,                                                                                                                                                           
       decode(D.ERN_CRED_FLG, S.ERN_CRED_FLG, D.ERN_CRED_FLG, S.ERN_CRED_FLG) ERN_CRED_FLG,                                                                                                                                                                     
       decode(D.INCLUDE_GPA_FLG, S.INCLUDE_GPA_FLG, D.INCLUDE_GPA_FLG, S.INCLUDE_GPA_FLG) INCLUDE_GPA_FLG,                                                                                                                                                      
       decode(D.GRADE_PTS, S.GRADE_PTS, D.GRADE_PTS, S.GRADE_PTS) GRADE_PTS,                                                                                                                                                                                    
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_GRADE D                                                                                                                                                                                                                  
    on D.GRADE_SID <> 2147483646                                                                                                                                                                                                                                
   and D.SETID = S.SETID                                                                                                                                                                                                                                        
   and D.GRADE_SCHEME_CD = S.GRADE_SCHEME_CD                                                                                                                                                                                                                    
   and D.GRADE_BASIS_CD = S.GRADE_BASIS_CD                                                                                                                                                                                                                      
   and D.GRADE_CD = S.GRADE_CD                                                                                                                                                                                                                                  
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.SETID = S.SETID                                                                                                                                                                                                                                      
   and  T.GRADE_SCHEME_CD = S.GRADE_SCHEME_CD                                                                                                                                                                                                                   
   and  T.GRADE_BASIS_CD = S.GRADE_BASIS_CD                                                                                                                                                                                                                     
   and  T.GRADE_CD = S.GRADE_CD                                                                                                                                                                                                                                 
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                       
       T.GRADE_SD = S.GRADE_SD,                                                                                                                                                                                                                                 
       T.GRADE_LD = S.GRADE_LD,                                                                                                                                                                                                                                 
       T.GRADE_SCHEME_SD = S.GRADE_SCHEME_SD,                                                                                                                                                                                                                   
       T.GRADE_SCHEME_LD = S.GRADE_SCHEME_LD,                                                                                                                                                                                                                   
       T.GRADE_BASIS_SD = S.GRADE_BASIS_SD,                                                                                                                                                                                                                     
       T.GRADE_BASIS_LD = S.GRADE_BASIS_LD,                                                                                                                                                                                                                     
       T.GRADE_CTGRY_CD = S.GRADE_CTGRY_CD,                                                                                                                                                                                                                     
       T.GRADE_CTGRY_SD = S.GRADE_CTGRY_SD,                                                                                                                                                                                                                     
       T.GRADE_CTGRY_LD = S.GRADE_CTGRY_LD,                                                                                                                                                                                                                     
       T.ERN_CRED_FLG = S.ERN_CRED_FLG,                                                                                                                                                                                                                         
       T.INCLUDE_GPA_FLG = S.INCLUDE_GPA_FLG,                                                                                                                                                                                                                   
       T.GRADE_PTS = S.GRADE_PTS,                                                                                                                                                                                                                               
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                       
       decode(T.GRADE_SD,S.GRADE_SD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.GRADE_LD,S.GRADE_LD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.GRADE_SCHEME_SD,S.GRADE_SCHEME_SD,0,1) = 1 or                                                                                                                                                                                                   
       decode(T.GRADE_SCHEME_LD,S.GRADE_SCHEME_LD,0,1) = 1 or                                                                                                                                                                                                   
       decode(T.GRADE_BASIS_SD,S.GRADE_BASIS_SD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.GRADE_BASIS_LD,S.GRADE_BASIS_LD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.GRADE_CTGRY_CD,S.GRADE_CTGRY_CD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.GRADE_CTGRY_SD,S.GRADE_CTGRY_SD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.GRADE_CTGRY_LD,S.GRADE_CTGRY_LD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.ERN_CRED_FLG,S.ERN_CRED_FLG,0,1) = 1 or                                                                                                                                                                                                         
       decode(T.INCLUDE_GPA_FLG,S.INCLUDE_GPA_FLG,0,1) = 1 or                                                                                                                                                                                                   
       decode(T.GRADE_PTS,S.GRADE_PTS,0,1) = 1 or                                                                                                                                                                                                               
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.GRADE_SID,                                                                                                                                                                                                                                             
       T.SETID,                                                                                                                                                                                                                                                 
       T.GRADE_SCHEME_CD,                                                                                                                                                                                                                                       
       T.GRADE_BASIS_CD,                                                                                                                                                                                                                                        
       T.GRADE_CD,                                                                                                                                                                                                                                              
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.EFFDT,                                                                                                                                                                                                                                                 
       T.GRADE_SD,                                                                                                                                                                                                                                              
       T.GRADE_LD,                                                                                                                                                                                                                                              
       T.GRADE_SCHEME_SD,                                                                                                                                                                                                                                       
       T.GRADE_SCHEME_LD,                                                                                                                                                                                                                                       
       T.GRADE_BASIS_SD,                                                                                                                                                                                                                                        
       T.GRADE_BASIS_LD,                                                                                                                                                                                                                                        
       T.GRADE_CTGRY_CD,                                                                                                                                                                                                                                        
       T.GRADE_CTGRY_SD,                                                                                                                                                                                                                                        
       T.GRADE_CTGRY_LD,                                                                                                                                                                                                                                        
       T.ERN_CRED_FLG,                                                                                                                                                                                                                                          
       T.INCLUDE_GPA_FLG,                                                                                                                                                                                                                                       
       T.GRADE_PTS,                                                                                                                                                                                                                                             
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.GRADE_SID,                                                                                                                                                                                                                                             
       S.SETID,                                                                                                                                                                                                                                                 
       S.GRADE_SCHEME_CD,                                                                                                                                                                                                                                       
       S.GRADE_BASIS_CD,                                                                                                                                                                                                                                        
       S.GRADE_CD,                                                                                                                                                                                                                                              
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.EFFDT,                                                                                                                                                                                                                                                 
       S.GRADE_SD,                                                                                                                                                                                                                                              
       S.GRADE_LD,                                                                                                                                                                                                                                              
       S.GRADE_SCHEME_SD,                                                                                                                                                                                                                                       
       S.GRADE_SCHEME_LD,                                                                                                                                                                                                                                       
       S.GRADE_BASIS_SD,                                                                                                                                                                                                                                        
       S.GRADE_BASIS_LD,                                                                                                                                                                                                                                        
       S.GRADE_CTGRY_CD,                                                                                                                                                                                                                                        
       S.GRADE_CTGRY_SD,                                                                                                                                                                                                                                        
       S.GRADE_CTGRY_LD,                                                                                                                                                                                                                                        
       S.ERN_CRED_FLG,                                                                                                                                                                                                                                          
       S.INCLUDE_GPA_FLG,                                                                                                                                                                                                                                       
       S.GRADE_PTS,                                                                                                                                                                                                                                             
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;                                                                                                                                                                                                                                                         

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_GRADE rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_GRADE',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_GRADE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_GRADE';
update CSMRT_OWNER.PS_D_GRADE T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.GRADE_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_GRADE_TBL S
                    where T.SETID = S.SETID
                      and T.GRADE_SCHEME_CD = S.GRADING_SCHEME
                      and T.GRADE_BASIS_CD = S.GRADING_BASIS
                      and T.GRADE_CD = S.CRSE_GRADE_INPUT
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_GRADE rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_GRADE',
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

END PS_D_GRADE_P;
/
