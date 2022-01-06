CREATE OR REPLACE PROCEDURE             PS_D_ADMIT_TYPE_P AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_ADMIT_TYPE from PeopleSoft table PS_ADMIT_TYPE_TBL.
--
--V01   SMT-xxxx 10/27/2017,    James Doucette
--                              Converted from DataStage
--V01.1 SMT-xxxx 03/15/2018,    James Doucette
--                              Updated INSTITUTION field to INSTITUTION_CD.
--V01.2 SMT-8327 08/20/2019,    James Doucette
--                              Add New Admit Types to Admit Type Group.
--V03 2/11/2021             -- Srikanth,Pabbu made changes to ADMIT_TYPE_SID field

------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_ADMIT_TYPE';
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


strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_ADMIT_TYPE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_ADMIT_TYPE';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_ADMIT_TYPE T                                                                                                                                                                                               
using (                                                                                                                                                                                                                                                         
  with Q1 as (  
select INSTITUTION INSTITUTION_ID, ADMIT_TYPE ADMIT_TYPE_ID, SRC_SYS_ID, EFFDT, EFF_STATUS EFF_STAT_CD, 
       DESCRSHORT ADMIT_TYPE_SD, DESCR ADMIT_TYPE_LD, 
       DATA_ORIGIN,  
       row_number() over (partition by INSTITUTION, ADMIT_TYPE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) TAB_ORDER
  from CSSTG_OWNER.PS_ADMIT_TYPE_TBL),
       S as (
select INSTITUTION_ID, ADMIT_TYPE_ID, SRC_SYS_ID, EFFDT, EFF_STAT_CD, 
       ADMIT_TYPE_SD, ADMIT_TYPE_LD, 
          CASE
             WHEN ADMIT_TYPE_ID IN ('FC','FCE','FCN', 'FIN', 'FNV','FP','FYR') THEN 'New Freshman'
             WHEN ADMIT_TYPE_ID IN ('ICE','ITR','MIU','MTR','SEC','SDG','TCE','TCN','TF','TIN','TJA','TNV','TP','TRN','UMI','UMT') THEN 'Transfer'
             WHEN ADMIT_TYPE_ID IN ('GAR','GMI','GPM','GRC','GRD','MGT','MIG','NDA','5YR') THEN 'Graduate'
             WHEN ADMIT_TYPE_ID IN ('CEC') THEN 'CE Certificates'
             WHEN ADMIT_TYPE_ID IN ('UCT') THEN 'Undergraduate Certificates'
             WHEN ADMIT_TYPE_ID IN ('CRT') THEN 'Continuing Education'
             WHEN ADMIT_TYPE_ID IN ('LFY', 'LTC', 'LTR') THEN 'Law'
             WHEN ADMIT_TYPE_ID IN ('NCE', 'NDG') THEN 'Non-degree'
             WHEN ADMIT_TYPE_ID IN ('-') THEN '-'
             ELSE 'Invalid / Not Available'
         END ADMIT_TYPE_GRP,
       DATA_ORIGIN  
  from Q1
 where TAB_ORDER = 1)                                                                                                                                                                                              
select nvl(D.ADMIT_TYPE_SID, --max(D.ADMIT_TYPE_SID) over (partition by 1) +  this code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/11/2021
(select nvl(max(ADMIT_TYPE_SID),0) from CSMRT_OWNER.PS_D_ADMIT_TYPE where ADMIT_TYPE_SID <> 2147483646) +                                                                                                                                                                                     
       row_number() over (partition by 1 order by D.ADMIT_TYPE_SID nulls first)) ADMIT_TYPE_SID,                                                                                                                                                                
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       nvl(D.INSTITUTION_CD, S.INSTITUTION_ID) INSTITUTION_ID,                                                                                                                                                                                                  
       nvl(D.ADMIT_TYPE_ID, S.ADMIT_TYPE_ID) ADMIT_TYPE_ID,                                                                                                                                                                                                     
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,                                                                                                                                                                                                        
       decode(D.EFF_STAT_CD, S.EFF_STAT_CD, D.EFF_STAT_CD, S.EFF_STAT_CD) EFF_STAT_CD,                                                                                                                                                                          
       decode(D.ADMIT_TYPE_SD, S.ADMIT_TYPE_SD, D.ADMIT_TYPE_SD, S.ADMIT_TYPE_SD) ADMIT_TYPE_SD,                                                                                                                                                                
       decode(D.ADMIT_TYPE_LD, S.ADMIT_TYPE_LD, D.ADMIT_TYPE_LD, S.ADMIT_TYPE_LD) ADMIT_TYPE_LD,                                                                                                                                                                
       decode(D.ADMIT_TYPE_GRP, S.ADMIT_TYPE_GRP, D.ADMIT_TYPE_GRP, S.ADMIT_TYPE_GRP) ADMIT_TYPE_GRP,                                                                                                                                                           
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_ADMIT_TYPE D                                                                                                                                                                                                             
    on D.ADMIT_TYPE_SID <> 2147483646                                                                                                                                                                                                                           
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
   and D.INSTITUTION_CD = S.INSTITUTION_ID                                                                                                                                                                                                                      
   and D.ADMIT_TYPE_ID = S.ADMIT_TYPE_ID                                                                                                                                                                                                                        
) S                                                                                                                                                                                                                                                             
    on (T.INSTITUTION_CD = S.INSTITUTION_ID                                                                                                                                                                                                                    
   and  T.ADMIT_TYPE_ID = S.ADMIT_TYPE_ID                                                                                                                                                                                                                       
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                       
       T.EFF_STAT_CD = S.EFF_STAT_CD,                                                                                                                                                                                                                           
       T.ADMIT_TYPE_SD = S.ADMIT_TYPE_SD,                                                                                                                                                                                                                       
       T.ADMIT_TYPE_LD = S.ADMIT_TYPE_LD,                                                                                                                                                                                                                       
       T.ADMIT_TYPE_GRP = S.ADMIT_TYPE_GRP,                                                                                                                                                                                                                     
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                       
       decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or                                                                                                                                                                                                           
       decode(T.ADMIT_TYPE_SD,S.ADMIT_TYPE_SD,0,1) = 1 or                                                                                                                                                                                                       
       decode(T.ADMIT_TYPE_LD,S.ADMIT_TYPE_LD,0,1) = 1 or                                                                                                                                                                                                       
       decode(T.ADMIT_TYPE_GRP,S.ADMIT_TYPE_GRP,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.ADMIT_TYPE_SID,                                                                                                                                                                                                                                        
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.INSTITUTION_CD,                                                                                                                                                                                                                                        
       T.ADMIT_TYPE_ID,                                                                                                                                                                                                                                         
       T.EFFDT,                                                                                                                                                                                                                                                 
       T.EFF_STAT_CD,                                                                                                                                                                                                                                           
       T.ADMIT_TYPE_SD,                                                                                                                                                                                                                                         
       T.ADMIT_TYPE_LD,                                                                                                                                                                                                                                         
       T.ADMIT_TYPE_GRP,                                                                                                                                                                                                                                        
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.ADMIT_TYPE_SID,                                                                                                                                                                                                                                        
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.INSTITUTION_ID,                                                                                                                                                                                                                                        
       S.ADMIT_TYPE_ID,                                                                                                                                                                                                                                         
       S.EFFDT,                                                                                                                                                                                                                                                 
       S.EFF_STAT_CD,                                                                                                                                                                                                                                           
       S.ADMIT_TYPE_SD,                                                                                                                                                                                                                                         
       S.ADMIT_TYPE_LD,                                                                                                                                                                                                                                         
       S.ADMIT_TYPE_GRP,                                                                                                                                                                                                                                        
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
; 

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ADMIT_TYPE rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ADMIT_TYPE',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );


strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_ADMIT_TYPE';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_ADMIT_TYPE';
update CSMRT_OWNER.PS_D_ADMIT_TYPE T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.ADMIT_TYPE_SID < 2147483646
   and not exists (select 1
                     from CSSTG_OWNER.PS_ADMIT_TYPE_TBL S
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.ADMIT_TYPE_ID = S.ADMIT_TYPE
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
                      and S.DATA_ORIGIN <> 'D')
;

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_ADMIT_TYPE rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_ADMIT_TYPE',
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

END PS_D_ADMIT_TYPE_P;
/
