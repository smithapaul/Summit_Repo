DROP PROCEDURE CSMRT_OWNER.PS_D_EXT_TST_CMPNT_P
/

--
-- PS_D_EXT_TST_CMPNT_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_EXT_TST_CMPNT_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_EXT_TST_CMPNT from PeopleSoft table PS_D_EXT_TST_CMPNT.
--
 --V01  SMT-xxxx 11/06/2017,    James Doucette
--                              Converted from DataStage
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_EXT_TST_CMPNT';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_EXT_TST_CMPNT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_EXT_TST_CMPNT';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_EXT_TST_CMPNT T                                                                                                                                                                                            
using (                                                                                                                                                                                                                                                         
  with X1 as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q0 as (  
select /*+ inline parallel(8) */ distinct 
       TEST_ID EXT_TST_ID, TEST_COMPONENT EXT_TST_CMPNT_ID, SRC_SYS_ID
  from CSSTG_OWNER.PS_STDNT_TEST_COMP
 where DATA_ORIGIN <> 'D'),
       Q1 as (  
select Q0.EXT_TST_ID, Q0.EXT_TST_CMPNT_ID, Q0.SRC_SYS_ID, 
       nvl(R.EFFDT,to_date('01-JAN-1900')) EFFDT, R.DESCRSHORT EXT_TST_CMPNT_SD, R.DESCR EXT_TST_CMPNT_LD, 
       nvl(R.MAX_SCORE,0) MAX_SCORE, nvl(R.MIN_SCORE,0) MIN_SCORE, 
       nvl(DATA_ORIGIN,'S') DATA_ORIGIN, 
       nvl(row_number() over (partition by Q0.EXT_TST_ID, Q0.EXT_TST_CMPNT_ID, Q0.SRC_SYS_ID
                              order by R.DATA_ORIGIN desc, (case when R.EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else R.EFFDT end) desc),1) Q_ORDER
  from Q0
  left outer join CSSTG_OWNER.PS_SA_TCMP_REL_TBL R
    on Q0.EXT_TST_ID = R.TEST_ID
   and Q0.EXT_TST_CMPNT_ID = R.TEST_COMPONENT
   and Q0.SRC_SYS_ID = R.SRC_SYS_ID),
       Q2 as (  
select TEST_ID EXT_TST_ID, SRC_SYS_ID, EFFDT, 
       DESCRSHORT EXT_TST_SD, DESCR EXT_TST_LD, TESTING_AGENCY TSTNG_AGNCY_ID,
       DATA_ORIGIN,  
       row_number() over (partition by TEST_ID, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_SA_TEST_TBL),
       Q3 as (  
select TEST_COMPONENT EXT_TST_CMPNT_ID, SRC_SYS_ID, EFFDT, 
       DESCRSHORT EXT_TST_CMPNT_SD, DESCR EXT_TST_CMPNT_LD, 
       DATA_ORIGIN,  
       row_number() over (partition by TEST_COMPONENT, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_SA_TEST_CMP_TBL),
       S as (
select Q1.EXT_TST_ID, Q1.EXT_TST_CMPNT_ID, Q1.SRC_SYS_ID, Q1.EFFDT,  
       nvl(Q2.EXT_TST_SD,'-') EXT_TST_SD, nvl(Q2.EXT_TST_LD,'-') EXT_TST_LD, 
--       nvl(Q3.EXT_TST_CMPNT_SD,'-') EXT_TST_CMPNT_SD, nvl(Q3.EXT_TST_CMPNT_LD,'-') EXT_TST_CMPNT_LD, 
       case when nvl(Q1.EXT_TST_CMPNT_SD,'-') = '-' then nvl(Q3.EXT_TST_CMPNT_SD,'-') else  nvl(Q1.EXT_TST_CMPNT_SD,'-') end EXT_TST_CMPNT_SD, 
       case when nvl(Q1.EXT_TST_CMPNT_LD,'-') = '-' then nvl(Q3.EXT_TST_CMPNT_LD,'-') else  nvl(Q1.EXT_TST_CMPNT_LD,'-') end EXT_TST_CMPNT_LD, 
       Q1.MAX_SCORE, Q1.MIN_SCORE, 
       nvl(Q2.TSTNG_AGNCY_ID,'-') TSTNG_AGNCY_ID, nvl(X1.XLATSHORTNAME,'-') TSTNG_AGNCY_SD, nvl(X1.XLATLONGNAME,'-') TSTNG_AGNCY_LD, 
       Q1.DATA_ORIGIN  
  from Q1
  left outer join Q2
    on Q1.EXT_TST_ID = Q2.EXT_TST_ID 
   and Q1.SRC_SYS_ID = Q2.SRC_SYS_ID
   and Q2.Q_ORDER = 1
  left outer join Q3
    on Q1.EXT_TST_CMPNT_ID = Q3.EXT_TST_CMPNT_ID 
   and Q1.SRC_SYS_ID = Q3.SRC_SYS_ID
   and Q3.Q_ORDER = 1
  left outer join X1
    on Q2.TSTNG_AGNCY_ID = X1.FIELDVALUE
   and Q2.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'TESTING_AGENCY' 
   and X1.X_ORDER = 1
 where Q1.Q_ORDER = 1) 
--select nvl(D.EXT_TST_CMPNT_SID, max(D.EXT_TST_CMPNT_SID) over (partition by 1) +                                                                                                                                                                                
--       row_number() over (partition by 1 order by D.EXT_TST_CMPNT_SID nulls first)) EXT_TST_CMPNT_SID,                                                                                                                                                          
select nvl(D.EXT_TST_CMPNT_SID, 
          (select nvl(max(EXT_TST_CMPNT_SID),0) from CSMRT_OWNER.PS_D_EXT_TST_CMPNT where EXT_TST_CMPNT_SID < 2147483646) + 
                  row_number() over (partition by 1 order by D.EXT_TST_CMPNT_SID nulls first)) EXT_TST_CMPNT_SID, 
       nvl(D.EXT_TST_ID, S.EXT_TST_ID) EXT_TST_ID,                                                                                                                                                                                                              
       nvl(D.EXT_TST_CMPNT_ID, S.EXT_TST_CMPNT_ID) EXT_TST_CMPNT_ID,                                                                                                                                                                                            
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,                                                                                                                                                                                                        
       decode(D.EXT_TST_SD, S.EXT_TST_SD, D.EXT_TST_SD, S.EXT_TST_SD) EXT_TST_SD,                                                                                                                                                                               
       decode(D.EXT_TST_LD, S.EXT_TST_LD, D.EXT_TST_LD, S.EXT_TST_LD) EXT_TST_LD,                                                                                                                                                                               
       decode(D.EXT_TST_CMPNT_SD, S.EXT_TST_CMPNT_SD, D.EXT_TST_CMPNT_SD, S.EXT_TST_CMPNT_SD) EXT_TST_CMPNT_SD,                                                                                                                                                 
       decode(D.EXT_TST_CMPNT_LD, S.EXT_TST_CMPNT_LD, D.EXT_TST_CMPNT_LD, S.EXT_TST_CMPNT_LD) EXT_TST_CMPNT_LD,                                                                                                                                                 
       decode(D.MAX_SCORE, S.MAX_SCORE, D.MAX_SCORE, S.MAX_SCORE) MAX_SCORE,                                                                                                                                                                                    
       decode(D.MIN_SCORE, S.MIN_SCORE, D.MIN_SCORE, S.MIN_SCORE) MIN_SCORE,                                                                                                                                                                                    
       decode(D.TSTNG_AGNCY_ID, S.TSTNG_AGNCY_ID, D.TSTNG_AGNCY_ID, S.TSTNG_AGNCY_ID) TSTNG_AGNCY_ID,                                                                                                                                                           
       decode(D.TSTNG_AGNCY_SD, S.TSTNG_AGNCY_SD, D.TSTNG_AGNCY_SD, S.TSTNG_AGNCY_SD) TSTNG_AGNCY_SD,                                                                                                                                                           
       decode(D.TSTNG_AGNCY_LD, S.TSTNG_AGNCY_LD, D.TSTNG_AGNCY_LD, S.TSTNG_AGNCY_LD) TSTNG_AGNCY_LD,                                                                                                                                                           
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_EXT_TST_CMPNT D                                                                                                                                                                                                          
    on D.EXT_TST_CMPNT_SID <> 2147483646                                                                                                                                                                                                                        
   and D.EXT_TST_ID = S.EXT_TST_ID                                                                                                                                                                                                                              
   and D.EXT_TST_CMPNT_ID = S.EXT_TST_CMPNT_ID                                                                                                                                                                                                                  
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.EXT_TST_ID = S.EXT_TST_ID                                                                                                                                                                                                                            
   and  T.EXT_TST_CMPNT_ID = S.EXT_TST_CMPNT_ID                                                                                                                                                                                                                 
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                       
       T.EXT_TST_SD = S.EXT_TST_SD,                                                                                                                                                                                                                             
       T.EXT_TST_LD = S.EXT_TST_LD,                                                                                                                                                                                                                             
       T.EXT_TST_CMPNT_SD = S.EXT_TST_CMPNT_SD,                                                                                                                                                                                                                 
       T.EXT_TST_CMPNT_LD = S.EXT_TST_CMPNT_LD,                                                                                                                                                                                                                 
       T.MAX_SCORE = S.MAX_SCORE,                                                                                                                                                                                                                               
       T.MIN_SCORE = S.MIN_SCORE,                                                                                                                                                                                                                               
       T.TSTNG_AGNCY_ID = S.TSTNG_AGNCY_ID,                                                                                                                                                                                                                     
       T.TSTNG_AGNCY_SD = S.TSTNG_AGNCY_SD,                                                                                                                                                                                                                     
       T.TSTNG_AGNCY_LD = S.TSTNG_AGNCY_LD,                                                                                                                                                                                                                     
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                       
       decode(T.EXT_TST_SD,S.EXT_TST_SD,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.EXT_TST_LD,S.EXT_TST_LD,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.EXT_TST_CMPNT_SD,S.EXT_TST_CMPNT_SD,0,1) = 1 or                                                                                                                                                                                                 
       decode(T.EXT_TST_CMPNT_LD,S.EXT_TST_CMPNT_LD,0,1) = 1 or                                                                                                                                                                                                 
       decode(T.MAX_SCORE,S.MAX_SCORE,0,1) = 1 or                                                                                                                                                                                                               
       decode(T.MIN_SCORE,S.MIN_SCORE,0,1) = 1 or                                                                                                                                                                                                               
       decode(T.TSTNG_AGNCY_ID,S.TSTNG_AGNCY_ID,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.TSTNG_AGNCY_SD,S.TSTNG_AGNCY_SD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.TSTNG_AGNCY_LD,S.TSTNG_AGNCY_LD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.EXT_TST_CMPNT_SID,                                                                                                                                                                                                                                     
       T.EXT_TST_ID,                                                                                                                                                                                                                                            
       T.EXT_TST_CMPNT_ID,                                                                                                                                                                                                                                      
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.EFFDT,                                                                                                                                                                                                                                                 
       T.EXT_TST_SD,                                                                                                                                                                                                                                            
       T.EXT_TST_LD,                                                                                                                                                                                                                                            
       T.EXT_TST_CMPNT_SD,                                                                                                                                                                                                                                      
       T.EXT_TST_CMPNT_LD,                                                                                                                                                                                                                                      
       T.MAX_SCORE,                                                                                                                                                                                                                                             
       T.MIN_SCORE,                                                                                                                                                                                                                                             
       T.TSTNG_AGNCY_ID,                                                                                                                                                                                                                                        
       T.TSTNG_AGNCY_SD,                                                                                                                                                                                                                                        
       T.TSTNG_AGNCY_LD,                                                                                                                                                                                                                                        
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.EXT_TST_CMPNT_SID,                                                                                                                                                                                                                                     
       S.EXT_TST_ID,                                                                                                                                                                                                                                            
       S.EXT_TST_CMPNT_ID,                                                                                                                                                                                                                                      
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.EFFDT,                                                                                                                                                                                                                                                 
       S.EXT_TST_SD,                                                                                                                                                                                                                                            
       S.EXT_TST_LD,                                                                                                                                                                                                                                            
       S.EXT_TST_CMPNT_SD,                                                                                                                                                                                                                                      
       S.EXT_TST_CMPNT_LD,                                                                                                                                                                                                                                      
       S.MAX_SCORE,                                                                                                                                                                                                                                             
       S.MIN_SCORE,                                                                                                                                                                                                                                             
       S.TSTNG_AGNCY_ID,                                                                                                                                                                                                                                        
       S.TSTNG_AGNCY_SD,                                                                                                                                                                                                                                        
       S.TSTNG_AGNCY_LD,                                                                                                                                                                                                                                        
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;                                                                                                                                                                                                                                                         

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_EXT_TST_CMPNT rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_EXT_TST_CMPNT',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_EXT_TST_CMPNT';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_EXT_TST_CMPNT';
update CSMRT_OWNER.PS_D_EXT_TST_CMPNT T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.EXT_TST_CMPNT_SID <> 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_SA_TCMP_REL_TBL S
                    where T.EXT_TST_ID = S.TEST_ID
                      and T.EXT_TST_CMPNT_ID = S.TEST_COMPONENT
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_EXT_TST_CMPNT rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_EXT_TST_CMPNT',
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

END PS_D_EXT_TST_CMPNT_P;
/
