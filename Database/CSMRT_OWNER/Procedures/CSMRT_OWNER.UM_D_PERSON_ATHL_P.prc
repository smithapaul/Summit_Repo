DROP PROCEDURE CSMRT_OWNER.UM_D_PERSON_ATHL_P
/

--
-- UM_D_PERSON_ATHL_P  (Procedure) 
--
CREATE OR REPLACE PROCEDURE CSMRT_OWNER."UM_D_PERSON_ATHL_P" AUTHID CURRENT_USER IS

------------------------------------------------------------------------
-- George Adams
-- Old Tables --UM_D_PERSON_ATHL / UM_D_PERSON_ATHL_VW, UM_D_PRSPCT_CAR_VW
-- Loads target table   -- UM_D_PERSON_ATHL
-- UM_D_PERSON_ATHL -- Dependent on PS_D_PERSON 
-- V01 4/4/2018         -- srikanth ,pabbu converted to proc from sql
--
------------------------------------------------------------------------

        strMartId                       Varchar2(50)    := 'CSW';
        strProcessName                  Varchar2(100)   := 'UM_D_PERSON_ATHL';
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

strMessage01    := 'Merging data into CSSTG_OWNER.UM_D_PERSON_ATHL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'merge into CSSTG_OWNER.UM_D_PERSON_ATHL'; 
merge /*+ use_hash(S,T) */ into CSMRT_OWNER.UM_D_PERSON_ATHL T 
using ( 
with X as (  
select /*+ inline */ 
       FIELDNAME, FIELDVALUE, EFFDT, SRC_SYS_ID, 
       XLATLONGNAME, XLATSHORTNAME, DATA_ORIGIN, 
       row_number() over (partition by FIELDNAME, FIELDVALUE, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) X_ORDER
  from CSSTG_OWNER.PSXLATITEM
 where DATA_ORIGIN <> 'D'),
       Q1 as (  
select /*+ inline */ 
       ATHL_PARTIC_CD, EFFDT, SRC_SYS_ID, 
       DESCR ATHL_PARTIC_LD, DESCRSHORT ATHL_PARTIC_SD, 
       DATA_ORIGIN, 
       row_number() over (partition by ATHL_PARTIC_CD, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_ATHL_PART_TBL
 where DATA_ORIGIN <> 'D'),
       Q2 as (  
select /*+ inline */ 
       EMPLID PERSON_ID, SPORT, EFFDT, SRC_SYS_ID, 
       ATHL_PARTIC_CD, NCAA_ELIGIBLE, CUR_PARTICIPANT, DESCRLONG, 
       DATA_ORIGIN, 
       row_number() over (partition by EMPLID, SPORT, SRC_SYS_ID
                              order by DATA_ORIGIN desc, (case when EFFDT > trunc(SYSDATE) then to_date('01-JAN-1800') else EFFDT end) desc) Q_ORDER
  from CSSTG_OWNER.PS_ATHL_PART_STAT
 where DATA_ORIGIN <> 'D'
   and substr(SPORT,1,1) in ('B','D','L')),
       S as (
select /*+ inline */ 
       Q2.PERSON_ID, Q2.SPORT, Q2.SRC_SYS_ID, 
       Q2.EFFDT, nvl(P.PERSON_SID,2147483646) PERSON_SID, decode(substr(Q2.SPORT,1,1),'B','UMBOS','D','UMDAR','L','UMLOW','-') INSTITUTION_CD, 
       nvl(X1.XLATSHORTNAME,'-') SPORT_SD, nvl(X1.XLATLONGNAME,'-') SPORT_LD, 
       Q2.ATHL_PARTIC_CD, Q1.ATHL_PARTIC_SD, Q1.ATHL_PARTIC_LD, Q2.NCAA_ELIGIBLE, Q2.CUR_PARTICIPANT, Q2.DESCRLONG, 
       case when Q2.CUR_PARTICIPANT = 'Y' then row_number() over (partition by Q2.PERSON_ID, Q2.SRC_SYS_ID order by decode(Q2.CUR_PARTICIPANT, 'Y', 1, 100), Q2.SPORT) 
            else 100
        end ATHL_ORDER, 
       Q2.DATA_ORIGIN 
  from Q2
  left outer join PS_D_PERSON P
    on Q2.PERSON_ID = P.PERSON_ID
   and Q2.SRC_SYS_ID = P.SRC_SYS_ID
  left outer join Q1
    on Q1.ATHL_PARTIC_CD = Q2.ATHL_PARTIC_CD 
   and Q1.SRC_SYS_ID = Q2.SRC_SYS_ID
   and Q1.Q_ORDER = 1
  left outer join X X1
    on Q2.SPORT = X1.FIELDVALUE
   and Q2.SRC_SYS_ID = X1.SRC_SYS_ID
   and X1.FIELDNAME = 'SPORT'
   and X1.X_ORDER = 1 
 where Q2.Q_ORDER = 1) 
--select nvl(D.PERSON_ID, max(D.PERSON_ID) over (partition by 1) + 
--       row_number() over (partition by 1 order by D.PERSON_ID nulls first)) PERSON_ID, 
select nvl(D.PERSON_ID, S.PERSON_ID) PERSON_ID, 
       nvl(D.SPORT, S.SPORT) SPORT, 
       nvl(D.SRC_SYS_ID, S.SRC_SYS_ID) SRC_SYS_ID, 
       decode(D.EFFDT, S.EFFDT, D.EFFDT, S.EFFDT) EFFDT, 
       decode(D.PERSON_SID, S.PERSON_SID, D.PERSON_SID, S.PERSON_SID) PERSON_SID, 
       decode(D.INSTITUTION_CD, S.INSTITUTION_CD, D.INSTITUTION_CD, S.INSTITUTION_CD) INSTITUTION_CD, 
       decode(D.SPORT_SD, S.SPORT_SD, D.SPORT_SD, S.SPORT_SD) SPORT_SD, 
       decode(D.SPORT_LD, S.SPORT_LD, D.SPORT_LD, S.SPORT_LD) SPORT_LD, 
       decode(D.ATHL_PARTIC_CD, S.ATHL_PARTIC_CD, D.ATHL_PARTIC_CD, S.ATHL_PARTIC_CD) ATHL_PARTIC_CD, 
       decode(D.ATHL_PARTIC_SD, S.ATHL_PARTIC_SD, D.ATHL_PARTIC_SD, S.ATHL_PARTIC_SD) ATHL_PARTIC_SD, 
       decode(D.ATHL_PARTIC_LD, S.ATHL_PARTIC_LD, D.ATHL_PARTIC_LD, S.ATHL_PARTIC_LD) ATHL_PARTIC_LD, 
       decode(D.NCAA_ELIGIBLE, S.NCAA_ELIGIBLE, D.NCAA_ELIGIBLE, S.NCAA_ELIGIBLE) NCAA_ELIGIBLE, 
       decode(D.CUR_PARTICIPANT, S.CUR_PARTICIPANT, D.CUR_PARTICIPANT, S.CUR_PARTICIPANT) CUR_PARTICIPANT, 
       decode(D.DESCRLONG, S.DESCRLONG, D.DESCRLONG, S.DESCRLONG) DESCRLONG, 
       decode(D.ATHL_ORDER, S.ATHL_ORDER, D.ATHL_ORDER, S.ATHL_ORDER) ATHL_ORDER, 
       decode(D.DATA_ORIGIN, S.DATA_ORIGIN, D.DATA_ORIGIN, S.DATA_ORIGIN) DATA_ORIGIN, 
       nvl(D.CREATED_EW_DTTM, SYSDATE) CREATED_EW_DTTM, 
       nvl(D.LASTUPD_EW_DTTM, SYSDATE) LASTUPD_EW_DTTM 
  from S
  left outer join CSMRT_OWNER.UM_D_PERSON_ATHL D 
    on D.PERSON_SID <> 2147483646  
   and D.PERSON_ID = S.PERSON_ID 
   and D.SPORT = S.SPORT 
   and D.SRC_SYS_ID = S.SRC_SYS_ID 
) S 
    on (T.PERSON_ID=S.PERSON_ID
   and  T.SPORT = S.SPORT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
   and  T.SRC_SYS_ID = S.SRC_SYS_ID)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
 when matched then update set                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
       T.EFFDT = S.EFFDT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       T.PERSON_SID = S.PERSON_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
       T.INSTITUTION_CD = S.INSTITUTION_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       T.SPORT_SD = S.SPORT_SD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.SPORT_LD = S.SPORT_LD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.ATHL_PARTIC_CD = S.ATHL_PARTIC_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       T.ATHL_PARTIC_SD = S.ATHL_PARTIC_SD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       T.ATHL_PARTIC_LD = S.ATHL_PARTIC_LD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       T.NCAA_ELIGIBLE = S.NCAA_ELIGIBLE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       T.CUR_PARTICIPANT = S.CUR_PARTICIPANT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
       T.DESCRLONG = S.DESCRLONG,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       T.ATHL_ORDER = S.ATHL_ORDER,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
       T.DATA_ORIGIN = S.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.LASTUPD_EW_DTTM = SYSDATE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
 where                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         
       decode(T.EFFDT,S.EFFDT,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       decode(T.PERSON_SID,S.PERSON_SID,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
       decode(T.INSTITUTION_CD,S.INSTITUTION_CD,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       decode(T.SPORT_SD,S.SPORT_SD,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       decode(T.SPORT_LD,S.SPORT_LD,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       decode(T.ATHL_PARTIC_CD,S.ATHL_PARTIC_CD,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       decode(T.ATHL_PARTIC_SD,S.ATHL_PARTIC_SD,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       decode(T.ATHL_PARTIC_LD,S.ATHL_PARTIC_LD,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    
       decode(T.NCAA_ELIGIBLE,S.NCAA_ELIGIBLE,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       decode(T.CUR_PARTICIPANT,S.CUR_PARTICIPANT,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
       decode(T.DESCRLONG,S.DESCRLONG,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
       decode(T.ATHL_ORDER,S.ATHL_ORDER,0,1) = 1 or                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
       decode(T.DATA_ORIGIN,S.DATA_ORIGIN,0,1) = 1                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  when not matched then                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
insert (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.PERSON_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
       T.SPORT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       T.EFFDT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       T.PERSON_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       T.INSTITUTION_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.SPORT_SD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
       T.SPORT_LD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
       T.ATHL_PARTIC_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.ATHL_PARTIC_SD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.ATHL_PARTIC_LD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       T.NCAA_ELIGIBLE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
       T.CUR_PARTICIPANT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       T.DESCRLONG,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
       T.ATHL_ORDER,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       T.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       T.CREATED_EW_DTTM,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       T.LASTUPD_EW_DTTM)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
values (                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.PERSON_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
       S.SPORT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       S.SRC_SYS_ID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       S.EFFDT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       S.PERSON_SID,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       S.INSTITUTION_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.SPORT_SD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
       S.SPORT_LD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
       S.ATHL_PARTIC_CD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.ATHL_PARTIC_SD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.ATHL_PARTIC_LD,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
       S.NCAA_ELIGIBLE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
       S.CUR_PARTICIPANT,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
       S.DESCRLONG,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
       S.ATHL_ORDER,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
       S.DATA_ORIGIN,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          
       SYSDATE,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
       SYSDATE);                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_ATHL rows merged: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_ATHL',
                i_Action            => 'MERGE',
                i_RowCount          => intRowCount
        );

strMessage01    := 'Updating DATA_ORIGIN on CSSTG_OWNER.UM_D_PERSON_ATHL';
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.UM_D_PERSON_ATHL';
update CSMRT_OWNER.UM_D_PERSON_ATHL T   
   set DATA_ORIGIN = 'D', 
       LASTUPD_EW_DTTM = SYSDATE 
 where T.DATA_ORIGIN <> 'D'
   and T.SPORT <> '-' 
   and not exists (select 1 
                     from CSSTG_OWNER.PS_ATHL_PART_STAT S  
                    where T.PERSON_ID = S.EMPLID 
                      and T.SPORT = S.SPORT 
                      and T.SRC_SYS_ID = S.SRC_SYS_ID 
                      and S.DATA_ORIGIN <> 'D');

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_ATHL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_ATHL',
                i_Action            => 'UPDATE',
                i_RowCount          => intRowCount
        );

strSqlCommand   := 'update DATA_ORIGIN on CSSTG_OWNER.UM_D_PERSON_ATHL';
update CSMRT_OWNER.UM_D_PERSON_ATHL T   
   set DATA_ORIGIN = 'D', 
       LASTUPD_EW_DTTM = SYSDATE 
 where T.DATA_ORIGIN <> 'D' 
   and not exists (select 1
                     from CSMRT_OWNER.PS_D_PERSON P 
                    where T.PERSON_ID = P.PERSON_ID 
                      and T.SRC_SYS_ID = P.SRC_SYS_ID 
                      and P.DATA_ORIGIN <> 'D'); 

strSqlCommand   := 'SET intRowCount';
intRowCount     := SQL%ROWCOUNT;

strSqlCommand := 'commit';
commit;

strMessage01    := '# of UM_D_PERSON_ATHL rows updated: ' || TO_CHAR(intRowCount,'999,999,999,999');
COMMON_OWNER.SMT_LOG.PUT_MESSAGE(i_Message => strMessage01);

strSqlCommand := 'SMT_PROCESS_LOG.PROCESS_DETAIL';
COMMON_OWNER.SMT_PROCESS_LOG.PROCESS_DETAIL
        (
                i_TargetTableName   => 'UM_D_PERSON_ATHL',
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

END UM_D_PERSON_ATHL_P;
/
