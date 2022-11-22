DROP PROCEDURE CSMRT_OWNER.PS_D_INSTITUTION_P
/

--
-- PS_D_INSTITUTION_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."PS_D_INSTITUTION_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
--
-- Loads stage table PS_D_INSTITUTION from PeopleSoft table PS_INSTITUTION_TBL.
--
 --V01  SMT-xxxx 11/08/2017,    James Doucette
--                              Converted from DataStage
-- V02 2/12/2021            --  Srikanth,Pabbu made changes to INSTITUTION_SID field
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'PS_D_INSTITUTION';
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

strMessage01    := 'Merging data into CSMRT_OWNER.PS_D_INSTITUTION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSMRT_OWNER.PS_D_INSTITUTION';
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.PS_D_INSTITUTION T                                                                                                                                                                                              
using (                                                                                                                                                                                                                                                         
  with Q1 as (  
select INSTITUTION INSTITUTION_CD, SRC_SYS_ID, EFFDT, EFF_STATUS EFF_STAT_CD, 
       DESCRSHORT INSTITUTION_SD, DESCR INSTITUTION_LD, DESCRFORMAL INSTITUTION_FD,
       ADDRESS1 ADDR1_LD, ADDRESS2 ADDR2_LD, ADDRESS3 ADDR3_LD, ADDRESS4 ADDR4_LD, 
       CITY CITY_NM, COUNTY CNTY_NM, STATE STATE_CD, POSTAL POSTAL_CD, GEO_CODE GEO_CD, COUNTRY CNTRY_CD,     
       DATA_ORIGIN,  
       row_number() over (partition by INSTITUTION, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_INSTITUTION_TBL),
       S as (
select INSTITUTION_CD, SRC_SYS_ID, EFFDT, EFF_STAT_CD, 
       INSTITUTION_SD, INSTITUTION_LD, INSTITUTION_FD, 
       ADDR1_LD, ADDR2_LD, ADDR3_LD, ADDR4_LD, 
       CITY_NM, CNTY_NM, STATE_CD, POSTAL_CD, GEO_CD, CNTRY_CD,   
       DATA_ORIGIN  
  from Q1
 where Q1.Q_ORDER = 1)                                                                                                                                                                                              
select nvl(D.INSTITUTION_SID, --max(D.INSTITUTION_SID) over (partition by 1) + This code does not ignore SID 2147483646 and below line will fix the issue and is added on 2/12/2021 
(select nvl(max(INSTITUTION_SID),0) from CSMRT_OWNER.PS_D_INSTITUTION where INSTITUTION_SID <> 2147483646) +                                                                                                                                                                                  
       row_number() over (partition by 1 order by D.INSTITUTION_SID nulls first)) INSTITUTION_SID,                                                                                                                                                              
       nvl(D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD,                                                                                                                                                                                                  
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID,                                                                                                                                                                                                              
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT,                                                                                                                                                                                                        
       decode(D.EFF_STAT_CD, S.EFF_STAT_CD, D.EFF_STAT_CD, S.EFF_STAT_CD) EFF_STAT_CD,                                                                                                                                                                          
       decode(D.INSTITUTION_SD, S.INSTITUTION_SD, D.INSTITUTION_SD, S.INSTITUTION_SD) INSTITUTION_SD,                                                                                                                                                           
       decode(D.INSTITUTION_LD, S.INSTITUTION_LD, D.INSTITUTION_LD, S.INSTITUTION_LD) INSTITUTION_LD,                                                                                                                                                           
       decode(D.INSTITUTION_FD, S.INSTITUTION_FD, D.INSTITUTION_FD, S.INSTITUTION_FD) INSTITUTION_FD,                                                                                                                                                           
       decode(D.ADDR1_LD, S.ADDR1_LD, D.ADDR1_LD, S.ADDR1_LD) ADDR1_LD,                                                                                                                                                                                         
       decode(D.ADDR2_LD, S.ADDR2_LD, D.ADDR2_LD, S.ADDR2_LD) ADDR2_LD,                                                                                                                                                                                         
       decode(D.ADDR3_LD, S.ADDR3_LD, D.ADDR3_LD, S.ADDR3_LD) ADDR3_LD,                                                                                                                                                                                         
       decode(D.ADDR4_LD, S.ADDR4_LD, D.ADDR4_LD, S.ADDR4_LD) ADDR4_LD,                                                                                                                                                                                         
       decode(D.CITY_NM, S.CITY_NM, D.CITY_NM, S.CITY_NM) CITY_NM,                                                                                                                                                                                              
       decode(D.CNTY_NM, S.CNTY_NM, D.CNTY_NM, S.CNTY_NM) CNTY_NM,                                                                                                                                                                                              
       decode(D.STATE_CD, S.STATE_CD, D.STATE_CD, S.STATE_CD) STATE_CD,                                                                                                                                                                                         
       decode(D.POSTAL_CD, S.POSTAL_CD, D.POSTAL_CD, S.POSTAL_CD) POSTAL_CD,                                                                                                                                                                                    
       decode(D.GEO_CD, S.GEO_CD, D.GEO_CD, S.GEO_CD) GEO_CD,                                                                                                                                                                                                   
       decode(D.CNTRY_CD, S.CNTRY_CD, D.CNTRY_CD, S.CNTRY_CD) CNTRY_CD,                                                                                                                                                                                         
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN,                                                                                                                                                                          
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM,                                                                                                                                                                                                         
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM                                                                                                                                                                                                          
  from S                                                                                                                                                                                                                                                        
  left outer join CSMRT_OWNER.PS_D_INSTITUTION D                                                                                                                                                                                                            
    on D.INSTITUTION_SID <> 2147483646                                                                                                                                                                                                                          
   and D.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                      
   and D.SRC_SYS_ID = S.SRC_SYS_ID                                                                                                                                                                                                                              
) S                                                                                                                                                                                                                                                             
    on  (T.INSTITUTION_CD = S.INSTITUTION_CD                                                                                                                                                                                                                    
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                            
 when matched then update set                                                                                                                                                                                                                                   
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                       
       T.EFF_STAT_CD = S.EFF_STAT_CD,                                                                                                                                                                                                                           
       T.INSTITUTION_SD = S.INSTITUTION_SD,                                                                                                                                                                                                                     
       T.INSTITUTION_LD = S.INSTITUTION_LD,                                                                                                                                                                                                                     
       T.INSTITUTION_FD = S.INSTITUTION_FD,                                                                                                                                                                                                                     
       T.ADDR1_LD = S.ADDR1_LD,                                                                                                                                                                                                                                 
       T.ADDR2_LD = S.ADDR2_LD,                                                                                                                                                                                                                                 
       T.ADDR3_LD = S.ADDR3_LD,                                                                                                                                                                                                                                 
       T.ADDR4_LD = S.ADDR4_LD,                                                                                                                                                                                                                                 
       T.CITY_NM = S.CITY_NM,                                                                                                                                                                                                                                   
       T.CNTY_NM = S.CNTY_NM,                                                                                                                                                                                                                                   
       T.STATE_CD = S.STATE_CD,                                                                                                                                                                                                                                 
       T.POSTAL_CD = S.POSTAL_CD,                                                                                                                                                                                                                               
       T.GEO_CD = S.GEO_CD,                                                                                                                                                                                                                                     
       T.CNTRY_CD = S.CNTRY_CD,                                                                                                                                                                                                                                 
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                           
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                              
 where                                                                                                                                                                                                                                                          
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                       
       decode(T.EFF_STAT_CD,S.EFF_STAT_CD,0,1) = 1 or                                                                                                                                                                                                           
       decode(T.INSTITUTION_SD,S.INSTITUTION_SD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.INSTITUTION_LD,S.INSTITUTION_LD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.INSTITUTION_FD,S.INSTITUTION_FD,0,1) = 1 or                                                                                                                                                                                                     
       decode(T.ADDR1_LD,S.ADDR1_LD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.ADDR2_LD,S.ADDR2_LD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.ADDR3_LD,S.ADDR3_LD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.ADDR4_LD,S.ADDR4_LD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.CITY_NM,S.CITY_NM,0,1) = 1 or                                                                                                                                                                                                                   
       decode(T.CNTY_NM,S.CNTY_NM,0,1) = 1 or                                                                                                                                                                                                                   
       decode(T.STATE_CD,S.STATE_CD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.POSTAL_CD,S.POSTAL_CD,0,1) = 1 or                                                                                                                                                                                                               
       decode(T.GEO_CD,S.GEO_CD,0,1) = 1 or                                                                                                                                                                                                                     
       decode(T.CNTRY_CD,S.CNTRY_CD,0,1) = 1 or                                                                                                                                                                                                                 
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                              
  when not matched then                                                                                                                                                                                                                                         
insert (                                                                                                                                                                                                                                                        
       T.INSTITUTION_SID,                                                                                                                                                                                                                                       
       T.EFFDT,                                                                                                                                                                                                                                                 
       T.INSTITUTION_CD,                                                                                                                                                                                                                                        
       T.SRC_SYS_ID,                                                                                                                                                                                                                                            
       T.EFF_STAT_CD,                                                                                                                                                                                                                                           
       T.INSTITUTION_SD,                                                                                                                                                                                                                                        
       T.INSTITUTION_LD,                                                                                                                                                                                                                                        
       T.INSTITUTION_FD,                                                                                                                                                                                                                                        
       T.ADDR1_LD,                                                                                                                                                                                                                                              
       T.ADDR2_LD,                                                                                                                                                                                                                                              
       T.ADDR3_LD,                                                                                                                                                                                                                                              
       T.ADDR4_LD,                                                                                                                                                                                                                                              
       T.CITY_NM,                                                                                                                                                                                                                                               
       T.CNTY_NM,                                                                                                                                                                                                                                               
       T.STATE_CD,                                                                                                                                                                                                                                              
       T.POSTAL_CD,                                                                                                                                                                                                                                             
       T.GEO_CD,                                                                                                                                                                                                                                                
       T.CNTRY_CD,                                                                                                                                                                                                                                              
       T.DATA_ORIGIN,                                                                                                                                                                                                                                           
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                       
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                       
values (                                                                                                                                                                                                                                                        
       S.INSTITUTION_SID,                                                                                                                                                                                                                                       
       S.EFFDT,                                                                                                                                                                                                                                                 
       S.INSTITUTION_CD,                                                                                                                                                                                                                                        
       S.SRC_SYS_ID,                                                                                                                                                                                                                                            
       S.EFF_STAT_CD,                                                                                                                                                                                                                                           
       S.INSTITUTION_SD,                                                                                                                                                                                                                                        
       S.INSTITUTION_LD,                                                                                                                                                                                                                                        
       S.INSTITUTION_FD,                                                                                                                                                                                                                                        
       S.ADDR1_LD,                                                                                                                                                                                                                                              
       S.ADDR2_LD,                                                                                                                                                                                                                                              
       S.ADDR3_LD,                                                                                                                                                                                                                                              
       S.ADDR4_LD,                                                                                                                                                                                                                                              
       S.CITY_NM,                                                                                                                                                                                                                                               
       S.CNTY_NM,                                                                                                                                                                                                                                               
       S.STATE_CD,                                                                                                                                                                                                                                              
       S.POSTAL_CD,                                                                                                                                                                                                                                             
       S.GEO_CD,                                                                                                                                                                                                                                                
       S.CNTRY_CD,                                                                                                                                                                                                                                              
       S.DATA_ORIGIN,                                                                                                                                                                                                                                           
       SYSDATE,                                                                                                                                                                                                                                                 
       SYSDATE)
;                                                                                                                                                                                                                                                         

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_INSTITUTION rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_INSTITUTION',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSMRT_OWNER.PS_D_INSTITUTION';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSMRT_OWNER.PS_D_INSTITUTION';
update CSMRT_OWNER.PS_D_INSTITUTION T
   set DATA_ORIGIN = 'D',
       LASTUPD_EW_DTTM = SYSDATE
 where DATA_ORIGIN <> 'D'
   and T.INSTITUTION_SID < 2147483646
   and not exists (select 1  
                     from CSSTG_OWNER.PS_INSTITUTION_TBL S
                    where T.INSTITUTION_CD = S.INSTITUTION
                      and T.SRC_SYS_ID = S.SRC_SYS_ID
					  and S.DATA_ORIGIN <> 'D');

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of PS_D_INSTITUTION rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'PS_D_INSTITUTION',
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

END PS_D_INSTITUTION_P;
/
