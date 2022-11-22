DROP PROCEDURE CSMRT_OWNER.PS_D_AWD_P
/

--
-- PS_D_AWD_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_AWD_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_AWD from PeopleSoft table PS_D_AWD.
--
 --V01  SMT-xxxx 10/30/2017,    James Doucette
--                              Converted from DataStage
--V02 2/11/2021            -- Srikanth,Pabbu made changes to AWD_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_AWD';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_AWD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_AWD';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_AWD T                                                                                                                                                                                                      
using (                                                                                                                                                                                                                                                         
  with X1 as (  
select FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) XLAT_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as (  
select INSTITUTION INSTITUTION_CD, AWARD_CODE AWD_CD, SRC_SYS_ID, EFFDT, EFF_STATUS EFF_STAT_CD, 
       DESCRSHORT AWD_SD, DESCR AWD_LD, DESCRFORMAL AWD_FD, INTERNAL_EXTERNAL INT_EXT_CD, GRANTOR GRANTOR_NM, 
       DATA_ORIGIN,  
       row_number() over (partition by INSTITUTION, AWARD_CODE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) TAB_ORDER
  from CSSTG_OWNER.PS_HONOR_AWARD_TBL),
       S as (
select Q1.INSTITUTION_CD, Q1.AWD_CD, Q1.SRC_SYS_ID, Q1.EFFDT, Q1.EFF_STAT_CD, 
       Q1.AWD_SD, Q1.AWD_LD, Q1.AWD_FD, Q1.INT_EXT_CD, nvl(X1.XLATSHORTNAME,'-') INT_EXT_SD, nvl(X1.XLATLONGNAME,'-') INT_EXT_LD, Q1.GRANTOR_NM, 
       Q1.DATA_ORIGIN  
  from Q1
  left outer join X1
    on Q1.INT_EXT_CD = X1.FIELDVALUE
   and X1.FIELDNAME = 'INTERNAL_EXTERNAL' 
 where TAB_ORDER = 1)                                                                                                                                                                                              
select nvl(D.AWD_SID, --max(D.AWD_SID) over (partition by 1) +  This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/11/2021    
  (select nvl(max(AWD_SID),0) from CSMRT_OWNER.PS_D_AWD where AWD_SID <> 2147483646) +                                                                                                                                                                                             
       row_number() over (partition by 1 order by D.AWD_SID nulls first)) AWD_SID,                                                                                                                                                                              
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD,                                                                                                                                                                                                  
       nvl(D.AWD_CD, S.AWD_CD) AWD_CD,                                                                                                                                                                                                                          
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,                                                                                                                                                                                                        
       decode(D.EFF_STAT_CD, S.EFF_STAT_CD, D.EFF_STAT_CD, S.EFF_STAT_CD) EFF_STAT_CD,                                                                                                                                                                          
       decode(D.AWD_SD, S.AWD_SD, D.AWD_SD, S.AWD_SD) AWD_SD,                                                                                                                                                                                                   
       decode(D.AWD_LD, S.AWD_LD, D.AWD_LD, S.AWD_LD) AWD_LD,                                                                                                                                                                                                   
       decode(D.AWD_FD, S.AWD_FD, D.AWD_FD, S.AWD_FD) AWD_FD,                                                                                                                                                                                                   
       decode(D.INT_EXT_CD, S.INT_EXT_CD, D.INT_EXT_CD, S.INT_EXT_CD) INT_EXT_CD,                                                                                                                                                                               
       decode(D.INT_EXT_SD, S.INT_EXT_SD, D.INT_EXT_SD, S.INT_EXT_SD) INT_EXT_SD,                                                                                                                                                                               
       decode(D.INT_EXT_LD, S.INT_EXT_LD, D.INT_EXT_LD, S.INT_EXT_LD) INT_EXT_LD,                                                                                                                                                                               
       decode(D.GRANTOR_NM, S.GRANTOR_NM, D.GRANTOR_NM, S.GRANTOR_NM) GRANTOR_NM,                                                                                                                                                                               
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_AWD D                                                                                                                                                                                                                    
    on D.AWD_SID <> 2147483646                                                                                                                                                                                                                                  
   and D.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                      
   and D.AWD_CD = S.AWD_CD                                                                                                                                                                                                                                      
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                    
   and  T.AWD_CD = S.AWD_CD                                                                                                                                                                                                                                     
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                       
       T.EFF_STAT_CD = S.EFF_STAT_CD,                                                                                                                                                                                                                           
       T.AWD_SD = S.AWD_SD,                                                                                                                                                                                                                                     
       T.AWD_LD = S.AWD_LD,                                                                                                                                                                                                                                     
       T.AWD_FD = S.AWD_FD,                                                                                                                                                                                                                                     
       T.INT_EXT_CD = S.INT_EXT_CD,                                                                                                                                                                                                                             
       T.INT_EXT_SD = S.INT_EXT_SD,                                                                                                                                                                                                                             
       T.INT_EXT_LD = S.INT_EXT_LD,                                                                                                                                                                                                                             
       T.GRANTOR_NM = S.GRANTOR_NM,                                                                                                                                                                                                                             
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                       
       decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or                                                                                                                                                                                                           
       decode(T.AWD_SD,S.AWD_SD,0,1) = 1 or                                                                                                                                                                                                                     
       decode(T.AWD_LD,S.AWD_LD,0,1) = 1 or                                                                                                                                                                                                                     
       decode(T.AWD_FD,S.AWD_FD,0,1) = 1 or                                                                                                                                                                                                                     
       decode(T.INT_EXT_CD,S.INT_EXT_CD,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.INT_EXT_SD,S.INT_EXT_SD,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.INT_EXT_LD,S.INT_EXT_LD,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.GRANTOR_NM,S.GRANTOR_NM,0,1) = 1 or                                                                                                                                                                                                             
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.AWD_SID,                                                                                                                                                                                                                                               
       T.INSTITUTION_CD,                                                                                                                                                                                                                                        
       T.AWD_CD,                                                                                                                                                                                                                                                
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.EFFDT,                                                                                                                                                                                                                                                 
       T.EFF_STAT_CD,                                                                                                                                                                                                                                           
       T.AWD_SD,                                                                                                                                                                                                                                                
       T.AWD_LD,                                                                                                                                                                                                                                                
       T.AWD_FD,                                                                                                                                                                                                                                                
       T.INT_EXT_CD,                                                                                                                                                                                                                                            
       T.INT_EXT_SD,                                                                                                                                                                                                                                            
       T.INT_EXT_LD,                                                                                                                                                                                                                                            
       T.GRANTOR_NM,                                                                                                                                                                                                                                            
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.AWD_SID,                                                                                                                                                                                                                                               
       S.INSTITUTION_CD,                                                                                                                                                                                                                                        
       S.AWD_CD,                                                                                                                                                                                                                                                
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.EFFDT,                                                                                                                                                                                                                                                 
       S.EFF_STAT_CD,                                                                                                                                                                                                                                           
       S.AWD_SD,                                                                                                                                                                                                                                                
       S.AWD_LD,                                                                                                                                                                                                                                                
       S.AWD_FD,                                                                                                                                                                                                                                                
       S.INT_EXT_CD,                                                                                                                                                                                                                                            
       S.INT_EXT_SD,                                                                                                                                                                                                                                            
       S.INT_EXT_LD,                                                                                                                                                                                                                                            
       S.GRANTOR_NM,                                                                                                                                                                                                                                            
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;                                                                                                                                                                                                                                                

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_AWD rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_AWD',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_AWD';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_AWD';
update CSMRT_OWNER.PS_D_AWD T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.AWD_SID < 2147483646
   and not exists (select 1  
                     from CSSTG_OWNER.PS_HONOR_AWARD_TBL S
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.AWD_CD = S.AWARD_CODE
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_AWD rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_AWD',
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

END PS_D_AWD_P;
/
