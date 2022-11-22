DROP PROCEDURE CSMRT_OWNER.AM_PS_RQ_LN_DETL_TBL_P
/

--
-- AM_PS_RQ_LN_DETL_TBL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."AM_PS_RQ_LN_DETL_TBL_P" IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_RQ_LN_DETL_TBL from PeopleSoft table PS_RQ_LN_DETL_TBL.
--
-- V01  SMT-xxxx 05/15/2017,    Jim Doucette
--                              Converted from PS_RQ_LN_DETL_TBL.SQL
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'AM_PS_RQ_LN_DETL_TBL';
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

strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);


strSqlCommand   := 'update START_DT on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Reading',
       START_DT = sysdate,
       END_DT = NULL
 where TABLE_NAME = 'PS_RQ_LN_DETL_TBL'
;

strSqlCommand := 'commit';
commit;


strSqlCommand   := 'update NEW_MAX_SCN on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Merging',
       NEW_MAX_SCN = (select /*+ full(S) */ max(ORA_ROWSCN) from SYSADM.PS_RQ_LN_DETL_TBL@AMSOURCE S)
 where TABLE_NAME = 'PS_RQ_LN_DETL_TBL'
;

strSqlCommand := 'commit';
commit;


strMessage01    := 'Merging data into AMSTG_OWNER.PS_RQ_LN_DETL_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into AMSTG_OWNER.PS_RQ_LN_DETL_TBL';
merge /*+ use_hash(S,T) */ into AMSTG_OWNER.PS_RQ_LN_DETL_TBL T                 
using (select /*+ full(S) */                                                    
     nvl(trim(REQUIREMENT),'-') REQUIREMENT,                                         
     to_date(to_char(case 
                     when EFFDT < '01-JAN-1800' then NULL 
                     else EFFDT 
                      end,'MM/DD/YYYY HH24:MI:SS'),'MM/DD/YYYY HH24:MI:SS') EFFDT,                               
     nvl(trim(RQ_LINE_KEY_NBR),'-') RQ_LINE_KEY_NBR,                                 
     nvl(RQ_LINE_DET_SEQ,0) RQ_LINE_DET_SEQ,                                         
     RQ_LINE_DET_TYPE RQ_LINE_DET_TYPE,                                              
     LIST_INCLUDE_MODE LIST_INCLUDE_MODE,                                            
     LIST_RECALL_MODE LIST_RECALL_MODE,                                              
     LIST_INTERP LIST_INTERP,                                                        
     RQRMNT_GROUP RQRMNT_GROUP,                                                      
     REF_REQUIREMENT REF_REQUIREMENT,                                                
     RQ_LINE_NBR RQ_LINE_NBR,                                                        
     REF_NUMBER REF_NUMBER,                                                          
     REF_DATA REF_DATA,                                                              
     COURSE_LIST COURSE_LIST,                                                        
     INSTITUTION INSTITUTION,                                                        
     ACAD_CAREER ACAD_CAREER,                                                        
     CONDITION_CODE CONDITION_CODE,                                                  
     CONDITION_OPERATOR CONDITION_OPERATOR,                                          
     CONDITION_DATA CONDITION_DATA,                                                  
     IGNORE_MSNG_TGT IGNORE_MSNG_TGT,                                                
     TEST_ID TEST_ID,                                                                
     TEST_COMPONENT TEST_COMPONENT,                                                  
     SCORE SCORE,                                                                    
     SAA_MAX_VALID_AGE SAA_MAX_VALID_AGE,                                            
     SAA_BEST_TEST_OPT SAA_BEST_TEST_OPT,                                            
     CRSE_ATTR CRSE_ATTR,                                                            
     CRSE_ATTR_VALUE CRSE_ATTR_VALUE,                                                
     DESCR254A DESCR254A                                                            
from SYSADM.PS_RQ_LN_DETL_TBL@AMSOURCE S                                        
where ORA_ROWSCN > (select OLD_MAX_SCN 
                      from AMSTG_OWNER.UM_STAGE_JOBS 
                     where TABLE_NAME = 'PS_RQ_LN_DETL_TBL')) S                                                                                                                            
   on (                                                                         
     T.REQUIREMENT = S.REQUIREMENT and                                               
     T.EFFDT = S.EFFDT and                                                           
     T.RQ_LINE_KEY_NBR = S.RQ_LINE_KEY_NBR and                                       
     T.RQ_LINE_DET_SEQ = S.RQ_LINE_DET_SEQ and                                       
     T.SRC_SYS_ID = 'CS90')                                                          
when matched then update set                                                    
    T.RQ_LINE_DET_TYPE = S.RQ_LINE_DET_TYPE,                                        
    T.LIST_INCLUDE_MODE = S.LIST_INCLUDE_MODE,                                      
    T.LIST_RECALL_MODE = S.LIST_RECALL_MODE,                                        
    T.LIST_INTERP = S.LIST_INTERP,                                                  
    T.RQRMNT_GROUP = S.RQRMNT_GROUP,                                                
    T.REF_REQUIREMENT = S.REF_REQUIREMENT,                                          
    T.RQ_LINE_NBR = S.RQ_LINE_NBR,                                                  
    T.REF_NUMBER = S.REF_NUMBER,                                                    
    T.REF_DATA = S.REF_DATA,                                                        
    T.COURSE_LIST = S.COURSE_LIST,                                                  
    T.INSTITUTION = S.INSTITUTION,                                                  
    T.ACAD_CAREER = S.ACAD_CAREER,                                                  
    T.CONDITION_CODE = S.CONDITION_CODE,                                            
    T.CONDITION_OPERATOR = S.CONDITION_OPERATOR,                                    
    T.CONDITION_DATA = S.CONDITION_DATA,                                            
    T.IGNORE_MSNG_TGT = S.IGNORE_MSNG_TGT,                                          
    T.TEST_ID = S.TEST_ID,                                                          
    T.TEST_COMPONENT = S.TEST_COMPONENT,                                            
    T.SCORE = S.SCORE,                                                              
    T.SAA_MAX_VALID_AGE = S.SAA_MAX_VALID_AGE,                                   
    T.SAA_BEST_TEST_OPT = S.SAA_BEST_TEST_OPT,                                      
    T.CRSE_ATTR = S.CRSE_ATTR,                                                      
    T.CRSE_ATTR_VALUE = S.CRSE_ATTR_VALUE,                                          
    T.DESCR254A = S.DESCR254A,                                                      
    T.DATA_ORIGIN = 'S',                                                            
    T.LASTUPD_EW_DTTM = sysdate,                                                    
    T.BATCH_SID   = 1234                                                            
where                                                                           
    nvl(trim(T.RQ_LINE_DET_TYPE),0) <> nvl(trim(S.RQ_LINE_DET_TYPE),0) or           
    nvl(trim(T.LIST_INCLUDE_MODE),0) <> nvl(trim(S.LIST_INCLUDE_MODE),0) or         
    nvl(trim(T.LIST_RECALL_MODE),0) <> nvl(trim(S.LIST_RECALL_MODE),0) or           
    nvl(trim(T.LIST_INTERP),0) <> nvl(trim(S.LIST_INTERP),0) or                     
    nvl(trim(T.RQRMNT_GROUP),0) <> nvl(trim(S.RQRMNT_GROUP),0) or                   
    nvl(trim(T.REF_REQUIREMENT),0) <> nvl(trim(S.REF_REQUIREMENT),0) or             
    nvl(trim(T.RQ_LINE_NBR),0) <> nvl(trim(S.RQ_LINE_NBR),0) or                     
    nvl(trim(T.REF_NUMBER),0) <> nvl(trim(S.REF_NUMBER),0) or                       
    nvl(trim(T.REF_DATA),0) <> nvl(trim(S.REF_DATA),0) or                           
    nvl(trim(T.COURSE_LIST),0) <> nvl(trim(S.COURSE_LIST),0) or                     
    nvl(trim(T.INSTITUTION),0) <> nvl(trim(S.INSTITUTION),0) or                     
    nvl(trim(T.ACAD_CAREER),0) <> nvl(trim(S.ACAD_CAREER),0) or                     
    nvl(trim(T.CONDITION_CODE),0) <> nvl(trim(S.CONDITION_CODE),0) or      
    nvl(trim(T.CONDITION_OPERATOR),0) <> nvl(trim(S.CONDITION_OPERATOR),0) or       
    nvl(trim(T.CONDITION_DATA),0) <> nvl(trim(S.CONDITION_DATA),0) or               
    nvl(trim(T.IGNORE_MSNG_TGT),0) <> nvl(trim(S.IGNORE_MSNG_TGT),0) or             
    nvl(trim(T.TEST_ID),0) <> nvl(trim(S.TEST_ID),0) or                             
    nvl(trim(T.TEST_COMPONENT),0) <> nvl(trim(S.TEST_COMPONENT),0) or               
    nvl(trim(T.SCORE),0) <> nvl(trim(S.SCORE),0) or                                 
    nvl(trim(T.SAA_MAX_VALID_AGE),0) <> nvl(trim(S.SAA_MAX_VALID_AGE),0) or         
    nvl(trim(T.SAA_BEST_TEST_OPT),0) <> nvl(trim(S.SAA_BEST_TEST_OPT),0) or         
    nvl(trim(T.CRSE_ATTR),0) <> nvl(trim(S.CRSE_ATTR),0) or                         
    nvl(trim(T.CRSE_ATTR_VALUE),0) <> nvl(trim(S.CRSE_ATTR_VALUE),0) or             
    nvl(trim(T.DESCR254A),0) <> nvl(trim(S.DESCR254A),0) or                         
    T.DATA_ORIGIN = 'D'                                                             
when not matched then                                                           
insert (                                                                        
    T.REQUIREMENT,                                                                  
    T.EFFDT,                                                                        
    T.RQ_LINE_KEY_NBR,                                                              
    T.RQ_LINE_DET_SEQ,                                                              
    T.SRC_SYS_ID,                                                                   
    T.RQ_LINE_DET_TYPE,                                                             
    T.LIST_INCLUDE_MODE,                                                            
    T.LIST_RECALL_MODE,                                                             
    T.LIST_INTERP,                                                                  
    T.RQRMNT_GROUP,                                                                 
    T.REF_REQUIREMENT,                                                              
    T.RQ_LINE_NBR,                                                                  
    T.REF_NUMBER,                                                                   
    T.REF_DATA,                                                                     
    T.COURSE_LIST,                                                                  
    T.INSTITUTION,                                                                  
    T.ACAD_CAREER,                                                                  
    T.CONDITION_CODE,                                                               
    T.CONDITION_OPERATOR,                                                           
    T.CONDITION_DATA,                                                               
    T.IGNORE_MSNG_TGT,                                                              
    T.TEST_ID,                                                                      
    T.TEST_COMPONENT,                                                               
    T.SCORE,                                                                        
    T.SAA_MAX_VALID_AGE,                                                            
    T.SAA_BEST_TEST_OPT,                                                            
    T.CRSE_ATTR,                                                                    
    T.CRSE_ATTR_VALUE,
    T.DESCR254A,                                                                    
    T.LOAD_ERROR,                                                                   
    T.DATA_ORIGIN,                                                                  
    T.CREATED_EW_DTTM,                                                              
    T.LASTUPD_EW_DTTM,                                                              
    T.BATCH_SID                                                                    
    )                                                                               
values (                                                                        
    S.REQUIREMENT,                                                                  
    S.EFFDT,                                                                        
    S.RQ_LINE_KEY_NBR,                                                              
    S.RQ_LINE_DET_SEQ,                                                              
    'CS90',                                                                         
    S.RQ_LINE_DET_TYPE,                                                             
    S.LIST_INCLUDE_MODE,                                                            
    S.LIST_RECALL_MODE,                                                             
    S.LIST_INTERP,                                                                  
    S.RQRMNT_GROUP,                                                                 
    S.REF_REQUIREMENT,                                                              
    S.RQ_LINE_NBR,                                                                  
    S.REF_NUMBER,      
    S.REF_DATA,                                                                     
    S.COURSE_LIST,                                                                  
    S.INSTITUTION,                                                                  
    S.ACAD_CAREER,                                                                  
    S.CONDITION_CODE,                                                               
    S.CONDITION_OPERATOR,                                                           
    S.CONDITION_DATA,                                                               
    S.IGNORE_MSNG_TGT,                                                              
    S.TEST_ID,                                                                      
    S.TEST_COMPONENT,                                                               
    S.SCORE,                                                                        
    S.SAA_MAX_VALID_AGE,                                                            
    S.SAA_BEST_TEST_OPT,                                                            
    S.CRSE_ATTR,                                                                    
    S.CRSE_ATTR_VALUE,                                                              
    S.DESCR254A,                                                                    
    'N',                                                                            
    'S',                                                                            
    sysdate,                                                                        
    sysdate,                                                                        
    1234);                                                                          



strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_RQ_LN_DETL_TBL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_RQ_LN_DETL_TBL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update TABLE_STATUS on AMSTG_OWNER.UM_STAGE_JOBS';
update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Deleting',
       OLD_MAX_SCN = NEW_MAX_SCN
 where TABLE_NAME = 'PS_RQ_LN_DETL_TBL';

strSqlCommand := 'commit';
commit;


strMessage01    := 'Updating DATA_ORIGIN on AMSTG_OWNER.PS_RQ_LN_DETL_TBL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on AMSTG_OWNER.PS_RQ_LN_DETL_TBL';
update AMSTG_OWNER.PS_RQ_LN_DETL_TBL T
        set T.DATA_ORIGIN = 'D',
               T.LASTUPD_EW_DTTM = SYSDATE
 where T.DATA_ORIGIN <> 'D'
   and exists 
(select 1 from
(select REQUIREMENT, EFFDT, RQ_LINE_KEY_NBR
   from AMSTG_OWNER.PS_RQ_LN_DETL_TBL T2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQ_LN_DETL_TBL') = 'Y'
  minus
 select REQUIREMENT, EFFDT, RQ_LINE_KEY_NBR
   from SYSADM.PS_RQ_LN_DETL_TBL@AMSOURCE S2
  where (select DELETE_FLG from AMSTG_OWNER.UM_STAGE_JOBS where TABLE_NAME = 'PS_RQ_LN_DETL_TBL') = 'Y'
   ) S
 where T.REQUIREMENT = S.REQUIREMENT
   and T.EFFDT = S.EFFDT
   and T.RQ_LINE_KEY_NBR = S.RQ_LINE_KEY_NBR
   and T.SRC_SYS_ID = 'CS90' 
   ) 
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_RQ_LN_DETL_TBL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_RQ_LN_DETL_TBL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating AMSTG_OWNER.UM_STAGE_JOBS';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update END_DT on AMSTG_OWNER.UM_STAGE_JOBS';

update AMSTG_OWNER.UM_STAGE_JOBS
   set TABLE_STATUS = 'Complete',
       END_DT = SYSDATE
 where TABLE_NAME = 'PS_RQ_LN_DETL_TBL'
;

strSqlCommand := 'commit';
commit;


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

END AM_PS_RQ_LN_DETL_TBL_P;
/
